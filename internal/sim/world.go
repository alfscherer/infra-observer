// Package sim simulates the lab: a set of infrastructure devices with mutable
// state, each served by a real SNMPv2c agent over UDP.
//
// The simulator is not a shortcut around the pipeline. The collector, its
// profiles and the real gosnmp client talk to these agents exactly as they
// would talk to a switch, so simulated telemetry exercises the same code path
// as real telemetry. Only the far end of the wire is fake.
//
// Scenarios (interface flaps, high CPU, outages, power loss...) change the
// simulated devices' state; they never inject observations.
package sim

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Options tune a World.
type Options struct {
	Seed      int64
	Community string
	// DropRate is the probability that an agent silently ignores a request, to
	// model intermittent packet loss. Zero disables it (tests); the lab uses a
	// small value so debouncing has something to absorb.
	DropRate float64
	Now      func() time.Time
}

// Iface is one simulated network port.
type Iface struct {
	Name      string
	AdminUp   bool
	OperUp    bool
	InOctets  uint64
	OutOctets uint64
	InErrors  uint64
	OutErrors uint64
	rate      float64 // bytes per second
	errRate   float64 // errors per second, raised by the packet-errors scenario
}

// Device is the mutable state behind one agent.
type Device struct {
	ID        string
	Type      domain.DeviceType
	Model     string
	Iface     []*Iface
	startedAt time.Time

	CPU      float64 // percent
	Temp     float64 // celsius
	Mem      float64 // percent
	Clients  int
	Battery  float64 // percent
	OnBatt   bool
	Offline  bool // agent stops answering
	AuthFail bool // agent answers with authorizationError

	cpuOverride  *float64
	tempOverride *float64
	rng          *rand.Rand
	flap         *flapState
}

type flapState struct {
	iface     *Iface
	remaining int
	interval  time.Duration
	next      time.Time
}

type scheduled struct {
	at time.Time
	fn func()
}

// World holds every simulated device.
type World struct {
	mu       sync.Mutex
	opts     Options
	devices  map[string]*Device
	timers   []scheduled
	lastTick time.Time
	rng      *rand.Rand
}

// NewWorld builds simulated devices for every inventory entry.
func NewWorld(devices []domain.Device, opts Options) *World {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Community == "" {
		opts.Community = "lab-ro"
	}
	w := &World{opts: opts, devices: map[string]*Device{}, lastTick: opts.Now(), rng: rand.New(rand.NewSource(opts.Seed))}
	for _, d := range devices {
		w.devices[d.ID] = newDevice(d, opts)
	}
	return w
}

func seedFor(id string, seed int64) int64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return int64(h.Sum64()) ^ seed
}

func newDevice(d domain.Device, opts Options) *Device {
	rng := rand.New(rand.NewSource(seedFor(d.ID, opts.Seed)))
	dev := &Device{
		ID: d.ID, Type: d.DeviceType, Model: d.Model, startedAt: opts.Now().Add(-time.Duration(3600+rng.Intn(86400*30)) * time.Second),
		CPU: 12 + rng.Float64()*10, Temp: 38 + rng.Float64()*6, Mem: 35 + rng.Float64()*20,
		Clients: 8 + rng.Intn(10), Battery: 100, rng: rng,
	}
	var names []string
	switch d.DeviceType {
	case domain.DeviceSwitch, domain.DeviceRouter:
		for i := 1; i <= 8; i++ {
			names = append(names, fmt.Sprintf("Gi0/%d", i))
		}
	case domain.DeviceAccessPoint:
		names = []string{"eth0", "wlan0", "wlan1"}
	}
	for _, n := range names {
		dev.Iface = append(dev.Iface, &Iface{
			Name: n, AdminUp: true, OperUp: true,
			InOctets: uint64(rng.Int63n(1 << 34)), OutOctets: uint64(rng.Int63n(1 << 34)),
			rate: 20_000 + rng.Float64()*900_000,
		})
	}
	return dev
}

// Device returns a simulated device for inspection (tests).
func (w *World) Device(id string) (*Device, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d, ok := w.devices[id]
	return d, ok
}

// IDs lists simulated device IDs.
func (w *World) IDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]string, 0, len(w.devices))
	for id := range w.devices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// With runs fn with the world locked, for tests that inspect device state.
func (w *World) With(fn func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fn()
}

// Tick advances simulated time to now: counters grow, values drift, flapping
// interfaces toggle and scheduled scenario reverts fire. It is called from a
// timer in the lab and directly, with a fake clock, in tests.
func (w *World) Tick(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	dt := now.Sub(w.lastTick).Seconds()
	if dt < 0 {
		dt = 0
	}
	w.lastTick = now
	for _, d := range w.devices {
		d.tick(now, dt)
	}
	// Run due timers; a timer may schedule more, so loop over a snapshot.
	var due []func()
	rest := w.timers[:0]
	for _, t := range w.timers {
		if !t.at.After(now) {
			due = append(due, t.fn)
		} else {
			rest = append(rest, t)
		}
	}
	w.timers = rest
	for _, fn := range due {
		fn()
	}
}

func (d *Device) tick(now time.Time, dt float64) {
	for _, i := range d.Iface {
		if i.OperUp && i.AdminUp {
			delta := i.rate * dt * (0.7 + 0.6*d.rng.Float64())
			i.InOctets += uint64(delta)
			i.OutOctets += uint64(delta * 0.8)
		}
		if i.errRate > 0 {
			i.InErrors += uint64(i.errRate * dt)
			i.OutErrors += uint64(i.errRate * dt / 2)
		}
	}
	if d.cpuOverride != nil {
		d.CPU = clamp(*d.cpuOverride+(d.rng.Float64()-0.5)*3, 0, 100)
	} else {
		d.CPU = clamp(d.CPU+(d.rng.Float64()-0.5)*4+(18-d.CPU)*0.1, 2, 60)
	}
	if d.tempOverride != nil {
		d.Temp = *d.tempOverride + (d.rng.Float64()-0.5)*1.5
	} else {
		d.Temp = clamp(d.Temp+(d.rng.Float64()-0.5)*0.6+(42-d.Temp)*0.05, 30, 55)
	}
	d.Mem = clamp(d.Mem+(d.rng.Float64()-0.5)*1.5+(45-d.Mem)*0.05, 10, 88)
	d.Clients = int(clamp(float64(d.Clients)+float64(d.rng.Intn(3)-1), 0, 60))
	if d.OnBatt {
		d.Battery = clamp(d.Battery-dt*0.4, 0, 100)
	} else {
		d.Battery = clamp(d.Battery+dt*0.8, 0, 100)
	}
	if f := d.flap; f != nil && f.remaining > 0 && !now.Before(f.next) {
		f.iface.OperUp = !f.iface.OperUp
		f.remaining--
		f.next = now.Add(f.interval)
		if f.remaining == 0 {
			f.iface.OperUp = true // a finished flap leaves the port up
			d.flap = nil
		}
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func (w *World) schedule(at time.Time, fn func()) {
	w.timers = append(w.timers, scheduled{at: at, fn: fn})
}
