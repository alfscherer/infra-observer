package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/secrets"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// cmdCollector runs the SNMP collector: poll devices, publish raw observations.
func cmdCollector(args []string) error {
	fs := flag.NewFlagSet("collector", flag.ContinueOnError)
	path := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Logging, "collector")
	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return err
	}
	registry := inventory.NewRegistry(devices)
	profiles, err := snmp.LoadProfiles(cfg.Collectors.SNMP.ProfilesDir)
	if err != nil {
		return err
	}
	for _, d := range devices {
		if d.Collection.Protocol == "snmp" && d.Collection.Profile != "" && d.Collection.Profile != "auto" {
			if _, ok := profiles.Get(d.Collection.Profile); !ok {
				return fmt.Errorf("device %s references unknown SNMP profile %q", d.ID, d.Collection.Profile)
			}
		}
	}

	ctx, stop := signalContext()
	defer stop()
	client, err := connectNATS(ctx, cfg, "collector", log)
	if err != nil {
		return err
	}
	defer client.Close()

	m := telemetry.New()
	serveObservability(ctx, cfg, log, m, health.NewChecker(version, natsCheck(client)))
	sn := cfg.Collectors.SNMP
	sched := &collector.Scheduler{OnPoll: m.OnPoll,
		Devices: registry.List,
		Poller: &snmp.Poller{
			Dialer:   snmp.GoSNMPDialer{Timeout: sn.Timeout, Retries: sn.Retries, MaxRepetitions: uint32(sn.MaxRepetitions)},
			Profiles: profiles,
			Secrets:  secrets.EnvFile{Dir: cfg.Secrets.Dir},
		},
		Sink: collector.NATSSink{Client: client}, Workers: sn.Workers, DefaultInterval: sn.DefaultInterval, Log: log,
	}
	log.Info("collector started", "devices", len(devices), "workers", sn.Workers, "profiles", profiles.IDs())
	if err := sched.Run(ctx); err != nil {
		return err
	}
	log.Info("collector stopped")
	return nil
}

// connectNATS connects and provisions streams, retrying while NATS is not yet
// reachable so processes can start in any order.
func connectNATS(ctx context.Context, cfg config.Config, name string, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) (*messaging.Client, error) {
	var lastErr error
	for attempt := 0; attempt < 60; attempt++ {
		c, err := messaging.Connect(cfg.NATS.URL, name, nil)
		if err == nil {
			ectx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = c.EnsureStreams(ectx)
			cancel()
			if err == nil {
				log.Info("connected to nats", "url", cfg.NATS.URL)
				return c, nil
			}
			c.Close()
		}
		lastErr = err
		log.Warn("nats not ready; retrying", "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("nats unavailable: %w", lastErr)
}
