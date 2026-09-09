package messaging

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkPublishSync(b *testing.B) {
	c, _ := newClient(&testing.T{})
	ctx := context.Background()
	payload := []byte(`{"observation_id":"x"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.Publish(ctx, SubjectRawSNMP, fmt.Sprint("s", i), payload, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishBatch50(b *testing.B) {
	c, _ := newClient(&testing.T{})
	ctx := context.Background()
	payload := []byte(`{"observation_id":"x"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msgs := make([]OutMsg, 50)
		for j := range msgs {
			msgs[j] = OutMsg{Subject: SubjectRawSNMP, MsgID: fmt.Sprint("b", i, "-", j), Payload: payload}
		}
		if err := c.PublishBatch(ctx, msgs); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(50*b.N)/b.Elapsed().Seconds(), "msgs/s")
}
