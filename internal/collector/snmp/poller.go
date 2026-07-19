package snmp

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// Standard system-group OIDs used for facts and auto profile matching.
const (
	oidSysDescr    = "1.3.6.1.2.1.1.1.0"
	oidSysObjectID = "1.3.6.1.2.1.1.2.0"
	oidSysName     = "1.3.6.1.2.1.1.5.0"
)

// Facts are slowly-changing identity details observed while polling. They feed
// the inventory.observed subject, not the metric stream.
type Facts struct {
	SysName     string
	SysDescr    string
	SysObjectID string
	ProfileID   string
}

// Result is everything a single poll produced.
type Result struct {
	Observations []domain.Observation
	Facts        Facts
	// Skipped counts values that were absent or of an unexpected type.
	Skipped int
}

// Poller executes profiles against devices. It holds no per-device state.
type Poller struct {
	Dialer        Dialer
	Profiles      *Profiles
	Secrets       secrets.Resolver
	MaxOIDsPerGet int // OIDs per GET request; batching keeps packets under the MTU
	Now           func() time.Time
}

func (p *Poller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Poll collects one cycle from dev. cycleID becomes the correlation ID of
// every observation produced, so a cycle can be traced end to end.
func (p *Poller) Poll(ctx context.Context, dev domain.Device, cycleID string) (Result, error) {
	var res Result
	secret, err := p.Secrets.Resolve(ctx, dev.CredentialsRef)
	if err != nil {
		return res, err
	}
	cred, err := CredentialFromSecret(secret)
	if err != nil {
		return res, err
	}
	sess, err := p.Dialer.Dial(ctx, dev.ManagementAddress, cred)
	if err != nil {
		return res, err
	}
	defer sess.Close()

	profile, err := p.selectProfile(ctx, sess, dev)
	if err != nil {
		return res, err
	}
	res.Facts.ProfileID = profile.ID
	observedAt := p.now().UTC()
	emit := func(m Metric, oid string, v any, labels map[string]string) {
		res.Observations = append(res.Observations, domain.Observation{
			ObservationID: domain.NewObservationID(cycleID, dev.ID, m.Name, labels),
			CorrelationID: cycleID,
			DeviceID:      dev.ID,
			Source:        "snmp",
			Metric:        m.Name,
			Value:         v,
			Labels:        labels,
			ObservedAt:    observedAt,
			Metadata:      map[string]string{"profile": profile.ID, "oid": oid},
		})
	}

	// Scalars: batched GETs.
	var scalars []Metric
	for _, m := range profile.Metrics {
		if m.OID != "" {
			scalars = append(scalars, m)
		}
	}
	batch := p.MaxOIDsPerGet
	if batch <= 0 {
		batch = 10
	}
	for start := 0; start < len(scalars); start += batch {
		end := min(start+batch, len(scalars))
		oids := make([]string, 0, end-start)
		for _, m := range scalars[start:end] {
			oids = append(oids, normOID(m.OID))
		}
		pdus, err := sess.Get(ctx, oids)
		if err != nil {
			return res, err
		}
		byOID := indexPDUs(pdus)
		for _, m := range scalars[start:end] {
			pdu, ok := byOID[normOID(m.OID)]
			if !ok || pdu.Kind == KindMissing {
				res.Skipped++
				continue
			}
			v, ok := decode(m.Type, pdu)
			if !ok {
				res.Skipped++
				continue
			}
			emit(m, normOID(m.OID), v, nil)
			switch normOID(m.OID) {
			case oidSysName:
				res.Facts.SysName, _ = v.(string)
			case oidSysDescr:
				res.Facts.SysDescr, _ = v.(string)
			case oidSysObjectID:
				res.Facts.SysObjectID, _ = v.(string)
			}
		}
	}

	// Tables: each distinct column is walked once per poll, even when shared.
	walks := map[string][]PDU{}
	walk := func(root string) ([]PDU, error) {
		root = normOID(root)
		if w, ok := walks[root]; ok {
			return w, nil
		}
		w, err := sess.Walk(ctx, root)
		if err != nil {
			return nil, err
		}
		walks[root] = w
		return w, nil
	}
	for _, m := range profile.Metrics {
		if m.Table == nil {
			continue
		}
		col, err := walk(m.Table.OID)
		if err != nil {
			return res, err
		}
		labelCols := map[string]map[string]string{}
		for label, oid := range m.Table.Labels {
			w, err := walk(oid)
			if err != nil {
				return res, err
			}
			labelCols[label] = rowStrings(w, oid)
		}
		indexLabel := m.Table.IndexLabel
		if indexLabel == "" {
			indexLabel = "index"
		}
		rootPrefix := normOID(m.Table.OID) + "."
		for _, pdu := range col {
			if pdu.Kind == KindMissing || !strings.HasPrefix(pdu.OID, rootPrefix) {
				continue
			}
			idx := strings.TrimPrefix(pdu.OID, rootPrefix)
			v, ok := decode(m.Type, pdu)
			if !ok {
				res.Skipped++
				continue
			}
			labels := map[string]string{indexLabel: idx}
			for label, rows := range labelCols {
				if s, ok := rows[idx]; ok {
					labels[label] = s
				}
			}
			emit(m, pdu.OID, v, labels)
		}
	}
	sort.SliceStable(res.Observations, func(i, j int) bool { return res.Observations[i].ObservationID < res.Observations[j].ObservationID })
	return res, nil
}

func (p *Poller) selectProfile(ctx context.Context, sess Session, dev domain.Device) (Profile, error) {
	id := dev.Collection.Profile
	if id != "" && id != "auto" {
		prof, ok := p.Profiles.Get(id)
		if !ok {
			return Profile{}, domain.Errorf(domain.CategoryValidation, "device %s: unknown profile %q", dev.ID, id)
		}
		return prof, nil
	}
	if id == "" {
		prof, _ := p.Profiles.Get("system")
		return prof, nil
	}
	pdus, err := sess.Get(ctx, []string{oidSysObjectID})
	if err != nil {
		return Profile{}, err
	}
	if len(pdus) == 1 && pdus[0].Kind == KindString {
		if prof, ok := p.Profiles.Match(pdus[0].Str); ok {
			return prof, nil
		}
	}
	prof, ok := p.Profiles.Get("system")
	if !ok {
		return Profile{}, domain.Errorf(domain.CategoryUnsupported, "device %s: no profile matches and no fallback", dev.ID)
	}
	return prof, nil
}

func indexPDUs(pdus []PDU) map[string]PDU {
	m := make(map[string]PDU, len(pdus))
	for _, p := range pdus {
		m[p.OID] = p
	}
	return m
}

// rowStrings maps a table row index to the string form of a label column value.
func rowStrings(pdus []PDU, colOID string) map[string]string {
	prefix := normOID(colOID) + "."
	out := make(map[string]string, len(pdus))
	for _, p := range pdus {
		if p.Kind == KindMissing || !strings.HasPrefix(p.OID, prefix) {
			continue
		}
		idx := strings.TrimPrefix(p.OID, prefix)
		if p.Kind == KindString {
			out[idx] = p.Str
		} else {
			out[idx] = strconv.FormatFloat(p.Num, 'f', -1, 64)
		}
	}
	return out
}

// decode converts a PDU into an observation value according to the profile type.
func decode(t ValueType, p PDU) (any, bool) {
	switch t {
	case TypeString:
		if p.Kind == KindString {
			return p.Str, true
		}
		return strconv.FormatFloat(p.Num, 'f', -1, 64), true
	case TypeDuration:
		if p.Kind == KindNumber {
			return p.Num / 100.0, true // TimeTicks are hundredths of a second
		}
	case TypeInteger, TypeGauge, TypeCounter:
		if p.Kind == KindNumber {
			return p.Num, true
		}
	}
	return nil, false
}
