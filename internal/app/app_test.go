package app

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
)

func TestWorkerOptionsFollowConfiguration(t *testing.T) {
	cfg := config.Default()
	cfg.Processing.Workers, cfg.Processing.QueueSize, cfg.Processing.MaxDeliver = 3, 7, 4
	cfg.Processing.AckWait, cfg.Processing.RetryDelay = 9*time.Second, 300*time.Millisecond
	o := WorkerOptions(cfg, nil, nil, "n", "S", "d", "f.>", DeviceKey)
	if o.Shards != 3 || o.QueueSize != 7 || o.MaxDeliver != 4 || o.AckWait != 9*time.Second || o.RetryDelay != 300*time.Millisecond ||
		o.Stream != "S" || o.Durable != "d" || o.FilterSubject != "f.>" || o.KeyFunc == nil || o.OnOutcome != nil {
		t.Fatalf("%+v", o)
	}
}
