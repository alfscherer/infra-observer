package snmp

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// Kind classifies a decoded PDU value.
type Kind int

const (
	KindMissing Kind = iota // noSuchObject, noSuchInstance, endOfMibView
	KindNumber
	KindString
)

// PDU is one variable binding, decoded away from any library type so the
// poller and its tests never depend on gosnmp.
type PDU struct {
	OID  string // no leading dot
	Kind Kind
	Num  float64 // integers, gauges, counters and raw TimeTicks
	Str  string
}

// Session is one connection to one device.
type Session interface {
	Get(ctx context.Context, oids []string) ([]PDU, error)
	Walk(ctx context.Context, root string) ([]PDU, error)
	Close() error
}

// Dialer opens sessions. The production implementation is GoSNMPDialer.
type Dialer interface {
	Dial(ctx context.Context, target string, cred Credential) (Session, error)
}

// Credential holds resolved SNMP credentials. It exists only for the duration
// of a poll and is never stored on the device or logged.
type Credential struct {
	Version        string // "2c" or "3"
	Community      string
	User           string
	Level          string // noAuthNoPriv, authNoPriv, authPriv
	AuthProtocol   string // MD5, SHA, SHA224, SHA256, SHA384, SHA512
	AuthPassphrase string
	PrivProtocol   string // DES, AES, AES192, AES256
	PrivPassphrase string
}

// String never reveals secrets.
func (Credential) String() string { return "snmp.Credential{redacted}" }

// CredentialFromSecret maps a resolved secret onto a Credential.
//
//	v2c: {"version":"2c","community":"..."}
//	v3:  {"version":"3","user":"..","level":"authPriv","auth_protocol":"SHA","auth_passphrase":"..","priv_protocol":"AES","priv_passphrase":".."}
func CredentialFromSecret(s secrets.Secret) (Credential, error) {
	c := Credential{
		Version: s.Get("version"), Community: s.Get("community"), User: s.Get("user"),
		Level: s.Get("level"), AuthProtocol: s.Get("auth_protocol"), AuthPassphrase: s.Get("auth_passphrase"),
		PrivProtocol: s.Get("priv_protocol"), PrivPassphrase: s.Get("priv_passphrase"),
	}
	if c.Version == "" {
		c.Version = "2c"
	}
	switch c.Version {
	case "2c":
		if c.Community == "" {
			return c, domain.Errorf(domain.CategoryValidation, "snmp v2c credential needs a community")
		}
	case "3":
		if c.User == "" {
			return c, domain.Errorf(domain.CategoryValidation, "snmp v3 credential needs a user")
		}
		if c.Level == "" {
			c.Level = "noAuthNoPriv"
		}
		switch c.Level {
		case "noAuthNoPriv":
		case "authNoPriv":
			if c.AuthPassphrase == "" {
				return c, domain.Errorf(domain.CategoryValidation, "snmp v3 %s needs auth_passphrase", c.Level)
			}
		case "authPriv":
			if c.AuthPassphrase == "" || c.PrivPassphrase == "" {
				return c, domain.Errorf(domain.CategoryValidation, "snmp v3 authPriv needs auth and priv passphrases")
			}
		default:
			return c, domain.Errorf(domain.CategoryValidation, "unknown snmp v3 level %q", c.Level)
		}
	default:
		return c, domain.Errorf(domain.CategoryValidation, "unsupported snmp version %q", c.Version)
	}
	return c, nil
}

// GoSNMPDialer opens real UDP sessions using gosnmp.
type GoSNMPDialer struct {
	Timeout        time.Duration
	Retries        int
	MaxRepetitions uint32
}

// SplitTarget separates host and port, defaulting to 161.
func SplitTarget(target string) (string, uint16, error) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return target, 161, nil // no port present
	}
	p, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || p == 0 {
		return "", 0, domain.Errorf(domain.CategoryValidation, "invalid port in management address %q", target)
	}
	return host, uint16(p), nil
}

func (d GoSNMPDialer) Dial(ctx context.Context, target string, cred Credential) (Session, error) {
	host, port, err := SplitTarget(target)
	if err != nil {
		return nil, err
	}
	g := &gosnmp.GoSNMP{
		Target: host, Port: port, Context: ctx,
		Timeout: d.Timeout, Retries: d.Retries, MaxRepetitions: d.MaxRepetitions,
	}
	switch cred.Version {
	case "2c":
		g.Version, g.Community = gosnmp.Version2c, cred.Community
	case "3":
		g.Version = gosnmp.Version3
		g.SecurityModel = gosnmp.UserSecurityModel
		usm := &gosnmp.UsmSecurityParameters{UserName: cred.User}
		switch cred.Level {
		case "authNoPriv":
			g.MsgFlags = gosnmp.AuthNoPriv
		case "authPriv":
			g.MsgFlags = gosnmp.AuthPriv
		default:
			g.MsgFlags = gosnmp.NoAuthNoPriv
		}
		if g.MsgFlags != gosnmp.NoAuthNoPriv {
			ap, err := authProtocol(cred.AuthProtocol)
			if err != nil {
				return nil, err
			}
			usm.AuthenticationProtocol, usm.AuthenticationPassphrase = ap, cred.AuthPassphrase
		}
		if g.MsgFlags == gosnmp.AuthPriv {
			pp, err := privProtocol(cred.PrivProtocol)
			if err != nil {
				return nil, err
			}
			usm.PrivacyProtocol, usm.PrivacyPassphrase = pp, cred.PrivPassphrase
		}
		g.SecurityParameters = usm
	default:
		return nil, domain.Errorf(domain.CategoryValidation, "unsupported snmp version %q", cred.Version)
	}
	if err := g.Connect(); err != nil {
		return nil, classify(err)
	}
	return &goSession{g: g}, nil
}

func authProtocol(name string) (gosnmp.SnmpV3AuthProtocol, error) {
	switch strings.ToUpper(name) {
	case "MD5":
		return gosnmp.MD5, nil
	case "SHA", "SHA1", "":
		return gosnmp.SHA, nil
	case "SHA224":
		return gosnmp.SHA224, nil
	case "SHA256":
		return gosnmp.SHA256, nil
	case "SHA384":
		return gosnmp.SHA384, nil
	case "SHA512":
		return gosnmp.SHA512, nil
	}
	return 0, domain.Errorf(domain.CategoryValidation, "unknown auth protocol %q", name)
}

func privProtocol(name string) (gosnmp.SnmpV3PrivProtocol, error) {
	switch strings.ToUpper(name) {
	case "DES":
		return gosnmp.DES, nil
	case "AES", "AES128", "":
		return gosnmp.AES, nil
	case "AES192":
		return gosnmp.AES192, nil
	case "AES256":
		return gosnmp.AES256, nil
	}
	return 0, domain.Errorf(domain.CategoryValidation, "unknown privacy protocol %q", name)
}

type goSession struct{ g *gosnmp.GoSNMP }

func (s *goSession) Close() error {
	if s.g.Conn != nil {
		return s.g.Conn.Close()
	}
	return nil
}

func (s *goSession) Get(ctx context.Context, oids []string) ([]PDU, error) {
	s.g.Context = ctx
	pkt, err := s.g.Get(oids)
	if err != nil {
		return nil, classify(err)
	}
	if pkt.Error != gosnmp.NoError {
		return nil, classifyStatus(pkt.Error)
	}
	return convert(pkt.Variables), nil
}

func (s *goSession) Walk(ctx context.Context, root string) ([]PDU, error) {
	s.g.Context = ctx
	var (
		vars []gosnmp.SnmpPDU
		err  error
	)
	if s.g.Version == gosnmp.Version1 {
		vars, err = s.g.WalkAll(root)
	} else {
		vars, err = s.g.BulkWalkAll(root)
	}
	if err != nil {
		return nil, classify(err)
	}
	return convert(vars), nil
}

func convert(vars []gosnmp.SnmpPDU) []PDU {
	out := make([]PDU, 0, len(vars))
	for _, v := range vars {
		p := PDU{OID: normOID(v.Name)}
		switch v.Type {
		case gosnmp.NoSuchObject, gosnmp.NoSuchInstance, gosnmp.EndOfMibView, gosnmp.Null:
			p.Kind = KindMissing
		case gosnmp.OctetString:
			p.Kind = KindString
			if b, ok := v.Value.([]byte); ok {
				p.Str = string(b)
			} else {
				p.Str = fmt.Sprint(v.Value)
			}
		case gosnmp.ObjectIdentifier, gosnmp.IPAddress:
			p.Kind, p.Str = KindString, normOID(fmt.Sprint(v.Value))
		default:
			if n, ok := number(v.Value); ok {
				p.Kind, p.Num = KindNumber, n
			} else {
				p.Kind, p.Str = KindString, fmt.Sprint(v.Value)
			}
		}
		out = append(out, p)
	}
	return out
}

func number(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// classify maps library errors onto the platform's error categories. SNMPv2c
// with a wrong community is indistinguishable from a dead device on the wire
// (the agent stays silent), so it surfaces as a timeout; v3 failures are
// explicit and become authentication errors.
func classify(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "wrong digest"), strings.Contains(msg, "unknown user"),
		strings.Contains(msg, "authentication"), strings.Contains(msg, "unknownusername"),
		strings.Contains(msg, "decryption"), strings.Contains(msg, "unsupported security level"):
		return domain.Wrap(domain.CategoryAuthentication, "snmp", err)
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline"), strings.Contains(msg, "i/o timeout"):
		return domain.Wrap(domain.CategoryTimeout, "snmp", err)
	}
	return domain.Wrap(domain.CategoryTransient, "snmp", err)
}

func classifyStatus(e gosnmp.SNMPError) error {
	err := fmt.Errorf("snmp error status %v", e)
	switch e {
	case gosnmp.NoAccess, gosnmp.AuthorizationError:
		return domain.Wrap(domain.CategoryAuthentication, "snmp", err)
	case gosnmp.NoSuchName:
		return domain.Wrap(domain.CategoryUnsupported, "snmp", err)
	}
	return domain.Wrap(domain.CategoryTransient, "snmp", err)
}
