package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/sim"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// cmdSimulator runs the lab: one SNMPv2c agent per inventory device plus a
// scenario control channel on NATS.
func cmdSimulator(args []string) error {
	fs := flag.NewFlagSet("simulator", flag.ContinueOnError)
	path := configFlag(fs)
	drop := fs.Float64("drop-rate", 0.02, "probability that an agent ignores a request (intermittent packet loss)")
	community := fs.String("community", envDefault("INFRA_OBSERVER_SIM_COMMUNITY", "lab-ro"), "SNMPv2c community the agents accept")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Logging, "simulator")
	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return err
	}
	world := sim.NewWorld(devices, sim.Options{Seed: cfg.Simulator.Seed, Community: *community, DropRate: *drop})

	ctx, stop := signalContext()
	defer stop()
	for _, d := range devices {
		_, port, err := net.SplitHostPort(d.ManagementAddress)
		if err != nil {
			return fmt.Errorf("device %s: management_address %q needs host:port for the simulator", d.ID, d.ManagementAddress)
		}
		a, err := world.Listen(ctx, d.ID, net.JoinHostPort(cfg.Simulator.BindHost, port), log)
		if err != nil {
			return fmt.Errorf("device %s: %w", d.ID, err)
		}
		log.Info("agent listening", "device_id", d.ID, "type", d.DeviceType, "addr", a.Addr().String(), "events", strings.Join(sim.Events(d.DeviceType), ","))
	}
	go world.RunClock(ctx, time.Second)

	client, err := connectNATS(ctx, cfg, "simulator", log)
	if err != nil {
		return err
	}
	defer client.Close()
	serveObservability(ctx, cfg, log, telemetry.New(), health.NewChecker(version, natsCheck(client)))
	sub, err := world.ServeControl(client.Conn())
	if err != nil {
		return err
	}
	defer sub.Unsubscribe() //nolint:errcheck
	log.Info("simulator ready", "control_subject", messaging.SubjectSimControl, "drop_rate", *drop)
	<-ctx.Done()
	log.Info("simulator stopped")
	return nil
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
