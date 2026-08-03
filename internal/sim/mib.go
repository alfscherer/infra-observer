package sim

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// varbind is one OID in a device's MIB view.
type varbind struct {
	oid []uint32
	pdu gosnmp.SnmpPDU
}

func parseOID(s string) []uint32 {
	s = strings.TrimPrefix(s, ".")
	parts := strings.Split(s, ".")
	out := make([]uint32, len(parts))
	for i, p := range parts {
		n, _ := strconv.ParseUint(p, 10, 32)
		out[i] = uint32(n)
	}
	return out
}

func formatOID(o []uint32) string {
	var sb strings.Builder
	for _, n := range o {
		sb.WriteByte('.')
		sb.WriteString(strconv.FormatUint(uint64(n), 10))
	}
	return sb.String()
}

func cmpOID(a, b []uint32) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// snapshot renders the device's current state as a sorted MIB view. The caller
// holds the world lock. Only standard MIB objects plus a private
// (documentation-range) enterprise subtree are served; see the SNMP profiles.
func (d *Device) snapshot(now time.Time) []varbind {
	var vb []varbind
	add := func(oid string, t gosnmp.Asn1BER, v any) {
		vb = append(vb, varbind{oid: parseOID(oid), pdu: gosnmp.SnmpPDU{Name: oid, Type: t, Value: v}})
	}
	str := func(oid, v string) { add(oid, gosnmp.OctetString, []byte(v)) }
	gauge := func(oid string, v float64) { add(oid, gosnmp.Gauge32, uint(max(v, 0))) }

	descr := fmt.Sprintf("infra-observer simulated %s %s", d.Type, d.Model)
	var objectID string
	switch d.Type {
	case domain.DeviceSwitch, domain.DeviceRouter:
		objectID = "1.3.6.1.4.1.32473.1.1.1"
	case domain.DeviceAccessPoint:
		objectID = "1.3.6.1.4.1.32473.2.1.1"
	case domain.DeviceUPS:
		objectID = "1.3.6.1.4.1.32473.4.1"
	default:
		objectID = "1.3.6.1.4.1.32473.3.1"
	}
	str("1.3.6.1.2.1.1.1.0", descr)
	add("1.3.6.1.2.1.1.2.0", gosnmp.ObjectIdentifier, objectID)
	add("1.3.6.1.2.1.1.3.0", gosnmp.TimeTicks, uint32(now.Sub(d.startedAt).Seconds()*100))
	str("1.3.6.1.2.1.1.5.0", d.ID)

	for idx, i := range d.Iface {
		n := strconv.Itoa(idx + 1)
		oper, admin := 2, 2
		if i.OperUp && i.AdminUp {
			oper = 1
		}
		if i.AdminUp {
			admin = 1
		}
		add("1.3.6.1.2.1.2.2.1.7."+n, gosnmp.Integer, admin)
		add("1.3.6.1.2.1.2.2.1.8."+n, gosnmp.Integer, oper)
		add("1.3.6.1.2.1.2.2.1.14."+n, gosnmp.Counter32, uint(i.InErrors%(1<<32)))
		add("1.3.6.1.2.1.2.2.1.20."+n, gosnmp.Counter32, uint(i.OutErrors%(1<<32)))
		str("1.3.6.1.2.1.31.1.1.1.1."+n, i.Name)
		add("1.3.6.1.2.1.31.1.1.1.6."+n, gosnmp.Counter64, i.InOctets)
		add("1.3.6.1.2.1.31.1.1.1.10."+n, gosnmp.Counter64, i.OutOctets)
	}

	switch d.Type {
	case domain.DeviceSwitch, domain.DeviceRouter:
		gauge("1.3.6.1.4.1.32473.1.1.1.0", d.CPU)
		gauge("1.3.6.1.4.1.32473.1.1.2.0", d.Temp)
	case domain.DeviceAccessPoint:
		gauge("1.3.6.1.4.1.32473.2.1.1.0", d.CPU)
		gauge("1.3.6.1.4.1.32473.2.1.2.0", d.Temp*10) // tenths of a degree, deliberately quirky
		gauge("1.3.6.1.4.1.32473.2.1.3.0", float64(d.Clients))
	case domain.DeviceUPS:
		batteryStatus, output := 2, 3
		if d.OnBatt {
			output = 5
		}
		if d.Battery < 30 {
			batteryStatus = 3
		}
		if d.Battery < 5 {
			batteryStatus = 4
		}
		add("1.3.6.1.2.1.33.1.2.1.0", gosnmp.Integer, batteryStatus)
		gauge("1.3.6.1.2.1.33.1.2.4.0", d.Battery)
		gauge("1.3.6.1.2.1.33.1.2.7.0", 24+d.Temp/10)
		add("1.3.6.1.2.1.33.1.4.1.0", gosnmp.Integer, output)
	default: // server, workstation, printer, generic
		gauge("1.3.6.1.2.1.25.3.3.1.2.1", d.CPU)
		gauge("1.3.6.1.2.1.25.3.3.1.2.2", d.CPU*0.9)
		gauge("1.3.6.1.4.1.32473.3.1.1.0", d.Mem)
	}
	sort.Slice(vb, func(i, j int) bool { return cmpOID(vb[i].oid, vb[j].oid) < 0 })
	return vb
}

// lookup finds an exact OID; ok is false when it is not in the view.
func lookup(view []varbind, oid []uint32) (varbind, bool) {
	i := sort.Search(len(view), func(i int) bool { return cmpOID(view[i].oid, oid) >= 0 })
	if i < len(view) && cmpOID(view[i].oid, oid) == 0 {
		return view[i], true
	}
	return varbind{}, false
}

// next returns the first varbind strictly after oid.
func next(view []varbind, oid []uint32) (varbind, bool) {
	i := sort.Search(len(view), func(i int) bool { return cmpOID(view[i].oid, oid) > 0 })
	if i < len(view) {
		return view[i], true
	}
	return varbind{}, false
}
