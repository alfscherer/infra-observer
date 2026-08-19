package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
)

// DeadLetterRecord is a dead letter together with its position in the
// dead-letter stream (used to replay or discard it).
type DeadLetterRecord struct {
	StreamSeq uint64
	DeadLetter
}

// ListDeadLetters returns up to limit dead letters, oldest first.
func (c *Client) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetterRecord, error) {
	stream, err := c.js.Stream(ctx, StreamDeadLetter)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "open dead-letter stream", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "dead-letter stream info", err)
	}
	var out []DeadLetterRecord
	if info.State.Msgs == 0 {
		return out, nil
	}
	for seq := max(info.State.FirstSeq, 1); seq <= info.State.LastSeq && len(out) < limit; seq++ {
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue // deleted (already replayed)
			}
			return nil, domain.Wrap(domain.CategoryDependency, "read dead letter", err)
		}
		var dl DeadLetter
		if err := json.Unmarshal(raw.Data, &dl); err != nil {
			continue // not one of ours; ignore
		}
		out = append(out, DeadLetterRecord{StreamSeq: seq, DeadLetter: dl})
	}
	return out, nil
}

// ReplayDeadLetter republishes a dead letter's original payload to its
// original subject and removes it from the dead-letter stream. Do this after
// the cause has been fixed; handlers are idempotent, so replaying something
// that partly succeeded before is safe.
func (c *Client) ReplayDeadLetter(ctx context.Context, streamSeq uint64) error {
	stream, err := c.js.Stream(ctx, StreamDeadLetter)
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "open dead-letter stream", err)
	}
	raw, err := stream.GetMsg(ctx, streamSeq)
	if err != nil {
		return domain.Wrap(domain.CategoryPermanent, "read dead letter", err)
	}
	var dl DeadLetter
	if err := json.Unmarshal(raw.Data, &dl); err != nil {
		return domain.Wrap(domain.CategoryValidation, "decode dead letter", err)
	}
	headers := map[string]string{"X-Replay": "true", "X-Replay-Of": dl.ID}
	for k, v := range dl.Headers {
		if len(v) > 0 && k != HeaderMsgID {
			headers[k] = v[0]
		}
	}
	// A fresh message id: the original may still sit inside the stream's
	// de-duplication window, which would silently swallow the replay.
	if err := c.Publish(ctx, dl.Subject, dl.ID+".replay."+strconv.FormatInt(time.Now().UnixNano(), 36), dl.Payload, headers); err != nil {
		return err
	}
	if err := stream.DeleteMsg(ctx, streamSeq); err != nil {
		return domain.Wrap(domain.CategoryDependency, "remove replayed dead letter", err)
	}
	return nil
}

// PurgeDeadLetters discards every dead letter.
func (c *Client) PurgeDeadLetters(ctx context.Context) error {
	stream, err := c.js.Stream(ctx, StreamDeadLetter)
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "open dead-letter stream", err)
	}
	if err := stream.Purge(ctx); err != nil {
		return domain.Wrap(domain.CategoryDependency, "purge dead letters", err)
	}
	return nil
}

type maxDeliveryAdvisory struct {
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
}

// WatchMaxDeliveries closes a gap the worker cannot: if a worker dies while
// handling a message's final allowed delivery, the server simply stops
// redelivering it and nobody records the failure. JetStream announces that
// with a "max deliveries" advisory; this subscribes to it and moves the
// message to the dead-letter stream, so nothing disappears silently.
func (c *Client) WatchMaxDeliveries(ctx context.Context, stream, consumer, deadLetterSubject string) error {
	if deadLetterSubject == "" {
		deadLetterSubject = SubjectDeadLetter
	}
	subject := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", stream, consumer)
	st, err := c.js.Stream(ctx, stream)
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "open stream "+stream, err)
	}
	sub, err := c.nc.Subscribe(subject, func(m *nats.Msg) {
		var adv maxDeliveryAdvisory
		if json.Unmarshal(m.Data, &adv) != nil || adv.StreamSeq == 0 {
			return
		}
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		raw, err := st.GetMsg(pctx, adv.StreamSeq)
		if err != nil {
			c.log.Warn("max-deliveries advisory for a message that no longer exists", "stream_seq", adv.StreamSeq, "error", err)
			return
		}
		dl := DeadLetter{
			Consumer: consumer, Stream: stream, Sequence: adv.StreamSeq, Subject: raw.Subject, Headers: map[string][]string(raw.Header),
			Payload: raw.Data, Error: "delivery attempts exhausted without the handler reporting a result (worker stopped mid-handling?)",
			Category: string(domain.CategoryTransient), Reason: "max_deliveries", Attempts: adv.Deliveries, FailedAt: time.Now().UTC(),
		}
		dl.ID = domain.StableID("dlq", consumer, stream, fmt.Sprint(adv.StreamSeq))
		payload, _ := json.Marshal(dl)
		if err := c.Publish(pctx, deadLetterSubject, dl.ID, payload, map[string]string{HeaderSchema: schema.DeadLetterV1}); err != nil {
			c.log.Error("cannot dead-letter max-deliveries message", "stream_seq", adv.StreamSeq, "error", err)
			return
		}
		c.log.Warn("message dead-lettered after exhausting deliveries", "stream", stream, "consumer", consumer, "stream_seq", adv.StreamSeq, "dead_letter_id", dl.ID)
	})
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "subscribe to advisories", err)
	}
	go func() { <-ctx.Done(); _ = sub.Unsubscribe() }()
	return nil
}

// ReplayStream republishes the messages of a stream window (since <= time <
// until; until may be zero for "now") matching filter to their original
// subjects. Replay is a recovery tool: after losing a database, or fixing a
// bug that dropped data, the pipeline is fed the retained window again. It is
// safe because consumers are idempotent: anything already applied is
// recognised as a duplicate and skipped. With dryRun it only counts.
func (c *Client) ReplayStream(ctx context.Context, streamName, filter string, since, until time.Time, dryRun bool) (int, error) {
	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		return 0, domain.Wrap(domain.CategoryDependency, "open stream "+streamName, err)
	}
	// Replay republishes into the very stream it reads. Without a fixed
	// horizon the consumer would keep finding (and replaying) its own output
	// forever, so stop at the last sequence that existed when the replay began.
	sinfo, err := stream.Info(ctx)
	if err != nil {
		return 0, domain.Wrap(domain.CategoryDependency, "stream info", err)
	}
	horizon := sinfo.State.LastSeq
	cfg := jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy, DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &since, FilterSubject: filter}
	cons, err := stream.CreateConsumer(ctx, cfg) // ephemeral: removed by the server when idle
	if err != nil {
		return 0, domain.Wrap(domain.CategoryDependency, "create replay consumer", err)
	}
	n := 0
	stamp := strconv.FormatInt(time.Now().Unix(), 36)
	for {
		batch, err := cons.FetchNoWait(200)
		if err != nil {
			return n, domain.Wrap(domain.CategoryDependency, "fetch for replay", err)
		}
		got := 0
		for m := range batch.Messages() {
			got++
			md, err := m.Metadata()
			if err != nil {
				continue
			}
			if md.Sequence.Stream > horizon || (!until.IsZero() && !md.Timestamp.Before(until)) {
				return n, nil
			}
			n++
			if dryRun {
				continue
			}
			headers := map[string]string{"X-Replay": "true"}
			id := ""
			for k, v := range m.Headers() {
				if len(v) == 0 {
					continue
				}
				if k == HeaderMsgID {
					id = v[0]
					continue
				}
				headers[k] = v[0]
			}
			if id == "" {
				id = fmt.Sprintf("seq-%d", md.Sequence.Stream)
			}
			if err := c.Publish(ctx, m.Subject(), id+".replay."+stamp, m.Data(), headers); err != nil {
				return n, err
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return n, domain.Wrap(domain.CategoryDependency, "replay batch", err)
		}
		if got == 0 {
			return n, nil
		}
	}
}
