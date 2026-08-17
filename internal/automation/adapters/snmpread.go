package adapters

import (
	"context"
	"strconv"
	"strings"

	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// Reader performs read-only SNMP queries with the same dialer, credential
// references and error categories as the collector. It never issues a SET.
type Reader struct {
	Dialer  snmp.Dialer
	Secrets secrets.Resolver
}

func (r Reader) session(ctx context.Context, dev domain.Device) (snmp.Session, error) {
	secret, err := r.Secrets.Resolve(ctx, dev.CredentialsRef)
	if err != nil {
		return nil, err
	}
	cred, err := snmp.CredentialFromSecret(secret)
	if err != nil {
		return nil, err
	}
	return r.Dialer.Dial(ctx, dev.ManagementAddress, cred)
}

// InterfaceState is the current status of one interface.
type InterfaceState struct {
	Index string
	Name  string
	Oper  string // up, down, ...
	Admin string
}

const (
	oidIfName  = "1.3.6.1.2.1.31.1.1.1.1"
	oidIfOper  = "1.3.6.1.2.1.2.2.1.8"
	oidIfAdmin = "1.3.6.1.2.1.2.2.1.7"
	oidSysName = "1.3.6.1.2.1.1.5.0"
	oidUptime  = "1.3.6.1.2.1.1.3.0"
	oidClients = "1.3.6.1.4.1.32473.2.1.3.0"
	oidCPURoot = "1.3.6.1.2.1.25.3.3.1.2"
	oidMemUsed = "1.3.6.1.4.1.32473.3.1.1.0"
)

// Interface looks up an interface by name and reads its state.
func (r Reader) Interface(ctx context.Context, dev domain.Device, name string) (InterfaceState, error) {
	s, err := r.session(ctx, dev)
	if err != nil {
		return InterfaceState{}, err
	}
	defer s.Close()
	names, err := s.Walk(ctx, oidIfName)
	if err != nil {
		return InterfaceState{}, err
	}
	var idx string
	for _, p := range names {
		if p.Kind == snmp.KindString && p.Str == name {
			idx = strings.TrimPrefix(p.OID, oidIfName+".")
		}
	}
	if idx == "" {
		return InterfaceState{}, domain.Errorf(domain.CategoryValidation, "device %s has no interface %q", dev.ID, name)
	}
	pdus, err := s.Get(ctx, []string{oidIfOper + "." + idx, oidIfAdmin + "." + idx})
	if err != nil {
		return InterfaceState{}, err
	}
	st := InterfaceState{Index: idx, Name: name, Oper: "unknown", Admin: "unknown"}
	for _, p := range pdus {
		if p.Kind != snmp.KindNumber {
			continue
		}
		switch {
		case strings.HasPrefix(p.OID, oidIfOper+"."):
			st.Oper = statusWord(p.Num)
		case strings.HasPrefix(p.OID, oidIfAdmin+"."):
			st.Admin = statusWord(p.Num)
		}
	}
	return st, nil
}

// Clients reads the number of associated wireless clients.
func (r Reader) Clients(ctx context.Context, dev domain.Device) (int, error) {
	s, err := r.session(ctx, dev)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	pdus, err := s.Get(ctx, []string{oidClients})
	if err != nil {
		return 0, err
	}
	if len(pdus) == 1 && pdus[0].Kind == snmp.KindNumber {
		return int(pdus[0].Num), nil
	}
	return 0, domain.Errorf(domain.CategoryUnsupported, "device %s does not report a client count", dev.ID)
}

// HostFacts is a diagnostic snapshot of a server or workstation.
type HostFacts struct {
	Name       string
	UptimeSecs int
	CPUPercent []string
	MemPercent string
}

// Host collects identity, uptime, per-CPU load and memory use.
func (r Reader) Host(ctx context.Context, dev domain.Device) (HostFacts, error) {
	s, err := r.session(ctx, dev)
	if err != nil {
		return HostFacts{}, err
	}
	defer s.Close()
	var f HostFacts
	pdus, err := s.Get(ctx, []string{oidSysName, oidUptime, oidMemUsed})
	if err != nil {
		return f, err
	}
	for _, p := range pdus {
		switch p.OID {
		case oidSysName:
			f.Name = p.Str
		case oidUptime:
			f.UptimeSecs = int(p.Num / 100)
		case oidMemUsed:
			if p.Kind == snmp.KindNumber {
				f.MemPercent = strconv.Itoa(int(p.Num))
			}
		}
	}
	cpus, err := s.Walk(ctx, oidCPURoot)
	if err != nil {
		return f, err
	}
	for _, p := range cpus {
		if p.Kind == snmp.KindNumber {
			f.CPUPercent = append(f.CPUPercent, strconv.Itoa(int(p.Num)))
		}
	}
	return f, nil
}
