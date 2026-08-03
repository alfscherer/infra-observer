package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/sim"
)

type argList map[string]string

func (a argList) String() string { return fmt.Sprint(map[string]string(a)) }
func (a argList) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("expected key=value, got %q", v)
	}
	a[k] = val
	return nil
}

// cmdScenario asks the running simulator to apply a scenario.
func cmdScenario(args []string) error {
	fs := flag.NewFlagSet("scenario", flag.ContinueOnError)
	path := configFlag(fs)
	device := fs.String("device", "", "simulated device id, e.g. switch-01")
	event := fs.String("event", "", "scenario event, e.g. interface-flap")
	dur := fs.Duration("duration", 0, "revert automatically after this long (0 = until reverted)")
	list := fs.Bool("list", false, "list the events each device supports")
	sargs := argList{}
	fs.Var(sargs, "arg", "scenario argument key=value (repeatable), e.g. --arg interface=Gi0/2")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *list {
		devices, err := inventory.LoadFile(cfg.Inventory.File)
		if err != nil {
			return err
		}
		for _, d := range devices {
			fmt.Printf("%-16s %-13s %s\n", d.ID, d.DeviceType, strings.Join(sim.Events(d.DeviceType), " "))
		}
		return nil
	}
	if *device == "" || *event == "" {
		return fmt.Errorf("usage: infra-observer scenario --device ID --event EVENT [--arg k=v] [--duration D]  (or --list)")
	}
	nc, err := nats.Connect(cfg.NATS.URL, nats.Name("scenario"), nats.Timeout(5*time.Second))
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "connect nats", err)
	}
	defer nc.Close()
	msg, err := sim.SendScenario(nc, sim.Scenario{Device: *device, Event: *event, Args: sargs, Duration: *dur}, 5*time.Second)
	if err != nil {
		return err
	}
	fmt.Println(msg)
	return nil
}
