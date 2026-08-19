package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/messaging"
)

func natsClient(cfgPath string) (*messaging.Client, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	return messaging.Connect(cfg.NATS.URL, "operator", newLogger(config.Logging{Level: "error", Format: "text"}, "cli"))
}

// cmdDeadLetter inspects and replays messages the platform could not process.
func cmdDeadLetter(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: infra-observer deadletter <list|replay|purge> [--config FILE]")
	}
	fs := flag.NewFlagSet("deadletter "+args[0], flag.ContinueOnError)
	cfgPath := configFlag(fs)
	limit := fs.Int("limit", 100, "list: maximum entries")
	all := fs.Bool("all", false, "replay: every dead letter")
	payload := fs.Bool("payload", false, "list: also print each payload")
	sub := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	client, err := natsClient(*cfgPath)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch sub {
	case "list":
		recs, err := client.ListDeadLetters(ctx, *limit)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "SEQ\tFAILED AT\tCONSUMER\tSUBJECT\tCATEGORY\tREASON\tATTEMPTS\tERROR")
		for _, r := range recs {
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", r.StreamSeq, r.FailedAt.Format(time.RFC3339), r.Consumer, r.Subject, r.Category, r.Reason, r.Attempts, truncate(r.Error, 80))
		}
		_ = tw.Flush()
		if *payload {
			for _, r := range recs {
				fmt.Printf("\n#%d %s\n%s\n", r.StreamSeq, r.Subject, r.Payload)
			}
		}
		fmt.Printf("\n%d dead letter(s)\n", len(recs))
		return nil
	case "replay":
		if *all {
			recs, err := client.ListDeadLetters(ctx, 100000)
			if err != nil {
				return err
			}
			for _, r := range recs {
				if err := client.ReplayDeadLetter(ctx, r.StreamSeq); err != nil {
					return fmt.Errorf("replay #%d: %w", r.StreamSeq, err)
				}
			}
			fmt.Printf("replayed %d dead letter(s)\n", len(recs))
			return nil
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("usage: infra-observer deadletter replay <seq> | --all")
		}
		seq, err := strconv.ParseUint(fs.Arg(0), 10, 64)
		if err != nil {
			return err
		}
		if err := client.ReplayDeadLetter(ctx, seq); err != nil {
			return err
		}
		fmt.Printf("replayed dead letter #%d\n", seq)
		return nil
	case "purge":
		if err := client.PurgeDeadLetters(ctx); err != nil {
			return err
		}
		fmt.Println("dead-letter stream purged")
		return nil
	}
	return fmt.Errorf("unknown deadletter command %q", sub)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// cmdReplay republishes a window of a stream so the pipeline processes it
// again. Consumers are idempotent, so this is safe; it is the tool for
// rebuilding after data loss.
func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	cfgPath := configFlag(fs)
	stream := fs.String("stream", messaging.StreamTelemetryRaw, "stream to replay")
	filter := fs.String("filter", "", "subject filter (default: the whole stream)")
	since := fs.Duration("since", time.Hour, "replay messages newer than this age")
	until := fs.Duration("until", 0, "stop at messages older than this age (0 = now)")
	dry := fs.Bool("dry-run", false, "only count what would be replayed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client, err := natsClient(*cfgPath)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	from := time.Now().Add(-*since)
	var to time.Time
	if *until > 0 {
		to = time.Now().Add(-*until)
	}
	n, err := client.ReplayStream(ctx, *stream, *filter, from, to, *dry)
	if err != nil {
		return err
	}
	verb := "replayed"
	if *dry {
		verb = "would replay"
	}
	fmt.Printf("%s %d message(s) from %s since %s\n", verb, n, *stream, from.Format(time.RFC3339))
	return nil
}
