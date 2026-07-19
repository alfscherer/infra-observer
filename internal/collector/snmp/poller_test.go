package snmp

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// fakeSession serves a static MIB view.
type fakeSession struct {
	mib      map[string]PDU
	gets     [][]string
	walks    []string
	failWith error
}

func (f *fakeSession) Close() error { return nil }

func (f *fakeSession) Get(_ context.Context, oids []string) ([]PDU, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	f.gets = append(f.gets, oids)
	out := make([]PDU, 0, len(oids))
	for _, o := range oids {
		if p, ok := f.mib[o]; ok {
			out = append(out, p)
		} else {
			out = append(out, PDU{OID: o, Kind: KindMissing})
		}
	}
	return out, nil
}

func (f *fakeSession) Walk(_ context.Context, root string) ([]PDU, error) {
	if f.failWith != nil {
		return nil, f.failWith
	}
	f.walks = append(f.walks, root)
	var out []PDU
	for o, p := range f.mib {
		if strings.HasPrefix(o, root+".") {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OID < out[j].OID })
	return out, nil
}

type fakeDialer struct {
	sess *fakeSession
	err  error
	cred Credential
}

func (d *fakeDialer) Dial(_ context.Context, _ string, c Credential) (Session, error) {
	d.cred = c
	return d.sess, d.err
}

func num(oid string, v float64) PDU { return PDU{OID: oid, Kind: KindNumber, Num: v} }
func str(oid, v string) PDU         { return PDU{OID: oid, Kind: KindString, Str: v} }

func testProfiles(t *testing.T) *Profiles {
	t.Helper()
	ps, err := LoadProfiles("../../../configs/profiles")
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

func switchMIB() map[string]PDU {
	m := map[string]PDU{}
	add := func(p PDU) { m[p.OID] = p }
	add(str("1.3.6.1.2.1.1.1.0", "Acme AS-24P"))
	add(str("1.3.6.1.2.1.1.2.0", "1.3.6.1.4.1.32473.1.1"))
	add(num("1.3.6.1.2.1.1.3.0", 123400)) // ticks
	add(str("1.3.6.1.2.1.1.5.0", "switch-01"))
	add(num("1.3.6.1.4.1.32473.1.1.1.0", 37))
	// no temperature OID: "if available"
	for i, name := range []string{"Gi0/1", "Gi0/2"} {
		idx := string(rune('1' + i))
		add(str("1.3.6.1.2.1.31.1.1.1.1."+idx, name))
		add(num("1.3.6.1.2.1.2.2.1.8."+idx, float64(1+i))) // 1 up, 2 down
		add(num("1.3.6.1.2.1.2.2.1.7."+idx, 1))
		add(num("1.3.6.1.2.1.31.1.1.1.6."+idx, 1000))
	}
	return m
}

func newPoller(t *testing.T, sess *fakeSession) (*Poller, *fakeDialer) {
	t.Helper()
	d := &fakeDialer{sess: sess}
	return &Poller{
		Dialer: d, Profiles: testProfiles(t),
		Secrets: secrets.Static{"snmp/lab": {"version": "2c", "community": "lab"}},
		Now:     func() time.Time { return time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC) },
	}, d
}

var swDevice = domain.Device{
	ID: "switch-01", Hostname: "switch-01", ManagementAddress: "h:161", DeviceType: domain.DeviceSwitch,
	CredentialsRef: "snmp/lab", Enabled: true, Collection: domain.CollectionSpec{Protocol: "snmp", Profile: "generic-switch"},
}

func find(obs []domain.Observation, metric, label, val string) *domain.Observation {
	for i := range obs {
		if obs[i].Metric == metric && (label == "" || obs[i].Labels[label] == val) {
			return &obs[i]
		}
	}
	return nil
}

func TestPollProducesScalarAndTableObservations(t *testing.T) {
	p, d := newPoller(t, &fakeSession{mib: switchMIB()})
	res, err := p.Poll(context.Background(), swDevice, "cycle-1")
	if err != nil {
		t.Fatal(err)
	}
	if d.cred.Community != "lab" {
		t.Fatalf("credential not resolved from reference: %+v", d.cred)
	}
	if up := find(res.Observations, "snmp.sysUpTime", "", ""); up == nil || up.Value != 1234.0 {
		t.Fatalf("sysUpTime should decode ticks to seconds, got %+v", up)
	}
	if n := find(res.Observations, "snmp.sysName", "", ""); n == nil || n.Value != "switch-01" {
		t.Fatalf("sysName: %+v", n)
	}
	oper := find(res.Observations, "snmp.ifOperStatus", "ifName", "Gi0/2")
	if oper == nil || oper.Value != 2.0 || oper.Labels["ifIndex"] != "2" {
		t.Fatalf("table row not labelled/decoded: %+v", oper)
	}
	if find(res.Observations, "snmp.temperatureCelsius", "", "") != nil {
		t.Fatal("unavailable metrics must be skipped, not fabricated")
	}
	if res.Skipped == 0 {
		t.Fatal("missing temperature should be counted as skipped")
	}
	if res.Facts.SysName != "switch-01" || res.Facts.ProfileID != "generic-switch" {
		t.Fatalf("facts: %+v", res.Facts)
	}
	for _, o := range res.Observations {
		if o.CorrelationID != "cycle-1" || o.DeviceID != "switch-01" || o.Source != "snmp" || o.ObservationID == "" {
			t.Fatalf("observation envelope incomplete: %+v", o)
		}
		if !o.ObservedAt.Equal(time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)) {
			t.Fatalf("observed_at: %v", o.ObservedAt)
		}
	}
}

func TestPollIsStableForSameCycleAndDiffersAcrossCycles(t *testing.T) {
	p, _ := newPoller(t, &fakeSession{mib: switchMIB()})
	a, _ := p.Poll(context.Background(), swDevice, "c1")
	b, _ := p.Poll(context.Background(), swDevice, "c1")
	c, _ := p.Poll(context.Background(), swDevice, "c2")
	if len(a.Observations) != len(b.Observations) {
		t.Fatal("same cycle should yield same observations")
	}
	for i := range a.Observations {
		if a.Observations[i].ObservationID != b.Observations[i].ObservationID {
			t.Fatal("observation ids must be deterministic within a cycle (duplicate detection depends on it)")
		}
		if a.Observations[i].ObservationID == c.Observations[i].ObservationID {
			t.Fatal("a new cycle must produce new observation ids")
		}
	}
}

func TestPollBatchesScalarRequestsAndSharesWalks(t *testing.T) {
	sess := &fakeSession{mib: switchMIB()}
	p, _ := newPoller(t, sess)
	p.MaxOIDsPerGet = 3
	if _, err := p.Poll(context.Background(), swDevice, "c"); err != nil {
		t.Fatal(err)
	}
	for _, g := range sess.gets {
		if len(g) > 3 {
			t.Fatalf("GET exceeded batch size: %v", g)
		}
	}
	if len(sess.gets) < 2 {
		t.Fatalf("expected multiple batched GETs, got %d", len(sess.gets))
	}
	seen := map[string]int{}
	for _, w := range sess.walks {
		seen[w]++
	}
	if seen["1.3.6.1.2.1.31.1.1.1.1"] != 1 {
		t.Fatalf("shared ifName column must be walked once per poll, walked %d times", seen["1.3.6.1.2.1.31.1.1.1.1"])
	}
}

func TestPollErrorPaths(t *testing.T) {
	p, d := newPoller(t, &fakeSession{mib: switchMIB()})
	dev := swDevice
	dev.CredentialsRef = "snmp/missing"
	if _, err := p.Poll(context.Background(), dev, "c"); domain.CategoryOf(err) != domain.CategoryAuthentication {
		t.Fatalf("missing credential: %v", err)
	}
	d.err = domain.Errorf(domain.CategoryTimeout, "no response")
	if _, err := p.Poll(context.Background(), swDevice, "c"); domain.CategoryOf(err) != domain.CategoryTimeout {
		t.Fatalf("dial timeout: %v", err)
	}
	d.err = nil
	d.sess.failWith = errors.New("boom")
	if _, err := p.Poll(context.Background(), swDevice, "c"); err == nil {
		t.Fatal("session failure must surface")
	}
	dev = swDevice
	dev.Collection.Profile = "nope"
	d.sess.failWith = nil
	if _, err := p.Poll(context.Background(), dev, "c"); domain.CategoryOf(err) != domain.CategoryValidation {
		t.Fatalf("unknown profile: %v", err)
	}
}

func TestAutoProfileSelectsBySysObjectID(t *testing.T) {
	p, _ := newPoller(t, &fakeSession{mib: switchMIB()})
	dev := swDevice
	dev.Collection.Profile = "auto"
	res, err := p.Poll(context.Background(), dev, "c")
	if err != nil {
		t.Fatal(err)
	}
	if res.Facts.ProfileID != "generic-switch" {
		t.Fatalf("auto matched %q", res.Facts.ProfileID)
	}
}

func TestCredentialValidation(t *testing.T) {
	cases := []struct {
		s  secrets.Secret
		ok bool
	}{
		{secrets.Secret{"community": "x"}, true},
		{secrets.Secret{"version": "2c"}, false},
		{secrets.Secret{"version": "3", "user": "u"}, true},
		{secrets.Secret{"version": "3", "user": "u", "level": "authPriv", "auth_passphrase": "a"}, false},
		{secrets.Secret{"version": "3", "user": "u", "level": "authPriv", "auth_passphrase": "aaaaaaaa", "priv_passphrase": "pppppppp"}, true},
		{secrets.Secret{"version": "1", "community": "x"}, false},
	}
	for i, c := range cases {
		_, err := CredentialFromSecret(c.s)
		if (err == nil) != c.ok {
			t.Errorf("case %d: err=%v want ok=%v", i, err, c.ok)
		}
	}
}

func TestProfileParsingAndIncludes(t *testing.T) {
	ps := testProfiles(t)
	sw, ok := ps.Get("generic-switch")
	if !ok {
		t.Fatal("generic-switch missing")
	}
	names := map[string]bool{}
	for _, m := range sw.Metrics {
		names[m.Name] = true
	}
	for _, want := range []string{"snmp.sysName", "snmp.ifOperStatus", "snmp.cpuLoadPercent"} {
		if !names[want] {
			t.Errorf("includes not resolved: %s missing", want)
		}
	}
	if p, ok := ps.Match(".1.3.6.1.4.1.32473.2.1.7"); !ok || p.ID != "vendor-ap" {
		t.Fatalf("sysObjectID match: %v %v", p.ID, ok)
	}
	for name, doc := range map[string]string{
		"bad oid":  "id: a\nmetrics:\n  - {name: x, oid: not-an-oid, type: gauge}\n",
		"bad type": "id: a\nmetrics:\n  - {name: x, oid: 1.3.6, type: quantum}\n",
		"both":     "id: a\nmetrics:\n  - {name: x, oid: 1.3.6, type: gauge, table: {oid: 1.3.6}}\n",
		"unknown":  "id: a\nwat: 1\n",
	} {
		if _, err := ParseProfiles([]byte(doc)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := ParseProfiles([]byte("id: a\ninclude: [b]\n"), []byte("id: b\ninclude: [a]\n")); err == nil {
		t.Fatal("include cycle must be rejected")
	}
}

func TestSplitTarget(t *testing.T) {
	h, p, err := SplitTarget("10.0.0.1")
	if err != nil || h != "10.0.0.1" || p != 161 {
		t.Fatalf("%v %v %v", h, p, err)
	}
	h, p, err = SplitTarget("sim:16101")
	if err != nil || h != "sim" || p != 16101 {
		t.Fatalf("%v %v %v", h, p, err)
	}
	if _, _, err = SplitTarget("sim:0"); err == nil {
		t.Fatal("port 0 must be rejected")
	}
}
