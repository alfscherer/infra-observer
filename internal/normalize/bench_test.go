package normalize

import "testing"

func BenchmarkNormalizeMapped(b *testing.B) {
	n, err := LoadFile("../../configs/normalization.yaml")
	if err != nil {
		b.Fatal(err)
	}
	o := raw("snmp.ifOperStatus", 1.0, map[string]string{"ifIndex": "1", "ifName": "Gi0/1"})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := n.Normalize(o); err != nil {
			b.Fatal(err)
		}
	}
}
