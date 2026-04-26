package cmd

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/jessegalley/mailyzer/internal/bench"
	"github.com/jessegalley/mailyzer/internal/mailbox"
	"github.com/spf13/cobra"
)

var flagMix int

var mixedrwCmd = &cobra.Command{
	Use:   "mixedrw [flags] <host>",
	Short: "Mixed read/write benchmark",
	Args:  cobra.ExactArgs(1),
	RunE:  runMixedrw,
}

func init() {
	mixedrwCmd.Flags().IntVar(&flagMix, "mix", 90, "percentage of operations that are reads (0-100)")
	rootCmd.AddCommand(mixedrwCmd)
}

func runMixedrw(cmd *cobra.Command, args []string) error {
	if flagMix < 0 || flagMix > 100 {
		return fmt.Errorf("--mix must be between 0 and 100, got %d", flagMix)
	}
	if flagMinSize > flagMaxSize {
		return fmt.Errorf("--min-size (%d) must be <= --max-size (%d)", flagMinSize, flagMaxSize)
	}

	host := args[0]
	cfg := newConnConfig(host)

	boxes, err := mailbox.Load(flagFile, flagUser, flagPassword)
	if err != nil {
		return err
	}

	fetchOpts, err := buildFetchOptions(flagFetchMode)
	if err != nil {
		return err
	}

	errWriter, closeErrLog := openErrLog()
	defer closeErrLog()

	from := senderAddr()
	pool := generateHexPool()
	m := bench.NewMetrics(errWriter)
	ctx, cancel := context.WithTimeout(context.Background(), flagDuration)
	defer cancel()

	log.Printf("mixedrw: host=%s users=%d concurrency=%d mailbox=%s mix=%d%% fetch=%s min=%d max=%d duration=%s",
		host, len(boxes), flagConcurrency, flagMailbox, flagMix, flagFetchMode, flagMinSize, flagMaxSize, flagDuration)

	var wg sync.WaitGroup
	for i := range boxes {
		mb := boxes[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			mixedrwUserSession(ctx, cfg, mb, from, pool, m, flagConcurrency, fetchOpts)
		}()
	}

	go progressTicker(ctx, m)
	wg.Wait()
	m.PrintSummary(os.Stdout)
	return nil
}

func mixedrwUserSession(
	ctx context.Context,
	cfg connConfig,
	mb mailbox.Mailbox,
	from string,
	pool []byte,
	m *bench.Metrics,
	concurrency int,
	fetchOpts *imap.FetchOptions,
) {
	client, selectData, err := connectAndLogin(cfg, mb)
	if err != nil {
		log.Printf("connect error for %s: %v", mb.Username, err)
		return
	}
	defer closeClient(client)

	if selectData.NumMessages == 0 {
		log.Printf("mailbox empty for %s, skipping", mb.Username)
		return
	}

	exists := selectData.NumMessages

	var innerWg sync.WaitGroup
	for i := range concurrency {
		seed := time.Now().UnixNano() + int64(i)
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			mixedrwLoop(ctx, client, exists, pool, mb.Username, from, m, cfg.cmdTimeout, seed, fetchOpts)
		}()
	}
	innerWg.Wait()
}

func mixedrwLoop(
	ctx context.Context,
	client *imapclient.Client,
	exists uint32,
	pool []byte,
	toAddr, from string,
	m *bench.Metrics,
	cmdTimeout time.Duration,
	seed int64,
	fetchOpts *imap.FetchOptions,
) {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var ok bool
		if rng.Intn(100) < flagMix {
			ok = mixedRead(ctx, client, exists, m, cmdTimeout, rng, fetchOpts)
		} else {
			ok = mixedWrite(ctx, client, toAddr, from, pool, m, cmdTimeout, rng)
		}
		if !ok {
			return
		}
	}
}

// mixedRead performs one random fetch. Returns false when the loop should stop
// (connection error, timeout, or context done).
func mixedRead(
	ctx context.Context,
	client *imapclient.Client,
	exists uint32,
	m *bench.Metrics,
	cmdTimeout time.Duration,
	rng *rand.Rand,
	fetchOpts *imap.FetchOptions,
) bool {
	seqNum := uint32(rng.Int63n(int64(exists))) + 1
	var seqSet imap.SeqSet
	seqSet.AddNum(seqNum)

	type result struct {
		bytes int64
		err   error
	}
	done := make(chan result, 1)
	t0 := time.Now()

	go func() {
		fetchCmd := client.Fetch(seqSet, fetchOpts)
		var b int64
		for {
			msg := fetchCmd.Next()
			if msg == nil {
				break
			}
			b += drainMessage(msg)
		}
		done <- result{bytes: b, err: fetchCmd.Close()}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			m.RecordError("fetch error (mixedrw seq=%d): %v", seqNum, res.err)
			return false
		}
		m.RecordOp(time.Since(t0), 1, res.bytes)
		return true
	case <-time.After(cmdTimeout):
		m.RecordError("fetch timeout (mixedrw seq=%d)", seqNum)
		return false
	case <-ctx.Done():
		return false
	}
}

// mixedWrite performs one IMAP APPEND. Returns false when the loop should stop.
func mixedWrite(
	ctx context.Context,
	client *imapclient.Client,
	toAddr, from string,
	pool []byte,
	m *bench.Metrics,
	cmdTimeout time.Duration,
	rng *rand.Rand,
) bool {
	seq := appendSeq.Add(1)
	msgBytes := buildMessage(seq, toAddr, from, pool, flagMinSize, flagMaxSize, rng)

	type result struct{ err error }
	done := make(chan result, 1)
	t0 := time.Now()

	go func() {
		cmd := client.Append(flagMailbox, int64(len(msgBytes)), nil)
		if _, err := cmd.Write(msgBytes); err != nil {
			_ = cmd.Close()
			done <- result{err}
			return
		}
		if err := cmd.Close(); err != nil {
			done <- result{err}
			return
		}
		_, err := cmd.Wait()
		done <- result{err}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			m.RecordError("append error (mixedrw seq=%d): %v", seq, res.err)
			return false
		}
		m.RecordOp(time.Since(t0), 1, int64(len(msgBytes)))
		return true
	case <-time.After(cmdTimeout):
		m.RecordError("append timeout (mixedrw seq=%d)", seq)
		return false
	case <-ctx.Done():
		return false
	}
}
