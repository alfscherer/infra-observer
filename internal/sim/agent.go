package sim

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/gosnmp/gosnmp"
)

// maxRepetitions caps GETBULK responses so they fit comfortably in a datagram.
const maxRepetitions = 40

// Agent is an SNMPv2c agent serving one simulated device.
type Agent struct {
	w    *World
	dev  *Device
	pc   net.PacketConn
	log  *slog.Logger
	done chan struct{}
}

// Addr is the agent's listening address.
func (a *Agent) Addr() net.Addr { return a.pc.LocalAddr() }

// Wait blocks until the agent has stopped.
func (a *Agent) Wait() { <-a.done }

// Listen starts an agent for deviceID on addr (e.g. "127.0.0.1:0"). It stops
// when ctx is cancelled.
func (w *World) Listen(ctx context.Context, deviceID, addr string, log *slog.Logger) (*Agent, error) {
	w.mu.Lock()
	dev, ok := w.devices[deviceID]
	w.mu.Unlock()
	if !ok {
		return nil, errors.New("unknown simulated device " + deviceID)
	}
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	a := &Agent{w: w, dev: dev, pc: pc, log: log.With("component", "sim-agent", "device_id", deviceID), done: make(chan struct{})}
	go func() {
		<-ctx.Done()
		pc.Close()
	}()
	go a.serve()
	return a, nil
}

func (a *Agent) serve() {
	defer close(a.done)
	buf := make([]byte, 65535)
	dec := &gosnmp.GoSNMP{Version: gosnmp.Version2c}
	for {
		n, from, err := a.pc.ReadFrom(buf)
		if err != nil {
			return
		}
		req, err := dec.SnmpDecodePacket(buf[:n])
		if err != nil {
			a.log.Debug("undecodable request dropped", "error", err)
			continue
		}
		resp := a.respond(req)
		if resp == nil {
			continue // silence: what a dead or non-authorised device looks like to a poller
		}
		out, err := resp.MarshalMsg()
		if err != nil {
			a.log.Warn("cannot encode response", "error", err)
			continue
		}
		_, _ = a.pc.WriteTo(out, from)
	}
}

// respond builds the response to req, or nil to stay silent.
func (a *Agent) respond(req *gosnmp.SnmpPacket) *gosnmp.SnmpPacket {
	w, d := a.w, a.dev
	w.mu.Lock()
	defer w.mu.Unlock()

	if d.Offline {
		return nil
	}
	if w.opts.DropRate > 0 && d.rng.Float64() < w.opts.DropRate {
		return nil
	}
	resp := &gosnmp.SnmpPacket{Version: gosnmp.Version2c, Community: req.Community, PDUType: gosnmp.GetResponse, RequestID: req.RequestID}
	if d.AuthFail {
		resp.Error, resp.Variables = gosnmp.AuthorizationError, req.Variables
		return resp
	}
	if req.Community != w.opts.Community {
		return nil // bad community: SNMPv2c agents do not answer
	}
	view := d.snapshot(w.opts.Now())

	switch req.PDUType {
	case gosnmp.GetRequest:
		for _, v := range req.Variables {
			if hit, ok := lookup(view, parseOID(v.Name)); ok {
				resp.Variables = append(resp.Variables, hit.pdu)
			} else {
				resp.Variables = append(resp.Variables, gosnmp.SnmpPDU{Name: v.Name, Type: gosnmp.NoSuchInstance})
			}
		}
	case gosnmp.GetNextRequest:
		for _, v := range req.Variables {
			resp.Variables = append(resp.Variables, nextPDU(view, parseOID(v.Name)))
		}
	case gosnmp.GetBulkRequest:
		nonRep := min(int(req.NonRepeaters), len(req.Variables))
		for _, v := range req.Variables[:nonRep] {
			resp.Variables = append(resp.Variables, nextPDU(view, parseOID(v.Name)))
		}
		cursors := make([][]uint32, 0, len(req.Variables)-nonRep)
		for _, v := range req.Variables[nonRep:] {
			cursors = append(cursors, parseOID(v.Name))
		}
		reps := min(int(req.MaxRepetitions), maxRepetitions)
		for r := 0; r < reps && len(cursors) > 0; r++ {
			allEnd := true
			for i, cur := range cursors {
				nv, ok := next(view, cur)
				if !ok {
					resp.Variables = append(resp.Variables, gosnmp.SnmpPDU{Name: formatOID(cur), Type: gosnmp.EndOfMibView})
					continue
				}
				allEnd = false
				resp.Variables = append(resp.Variables, nv.pdu)
				cursors[i] = nv.oid
			}
			if allEnd {
				break
			}
		}
	default:
		return nil
	}
	return resp
}

func nextPDU(view []varbind, oid []uint32) gosnmp.SnmpPDU {
	if nv, ok := next(view, oid); ok {
		return nv.pdu
	}
	return gosnmp.SnmpPDU{Name: formatOID(oid), Type: gosnmp.EndOfMibView}
}

// RunClock ticks the world every interval until ctx ends.
func (w *World) RunClock(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			w.Tick(now)
		}
	}
}
