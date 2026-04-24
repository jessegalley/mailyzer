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

var randreadCmd = &cobra.Command{
	Use:   "randread [flags] <host>",
	Short: "Random message read benchmark",
	Args:  cobra.ExactArgs(1),
	RunE:  runRandread,
}

func init() {
	rootCmd.AddCommand(randreadCmd)
}

func runRandread(cmd *cobra.Command, args []string) error {
	host := args[0]
	cfg := newConnConfig(host)

	boxes, err := mailbox.Load(flagFile, flagUser, flagPassword)
	if err != nil {
		return err
	}

	numUsers := len(boxes)
	adjustedHalfLife := flagHalfLife * time.Duration(numUsers)
	rl := bench.NewRateLimiter(adjustedHalfLife, flagThreshold)
	m := bench.NewMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), flagDuration)
	defer cancel()

	fmt.Fprintf(os.Stdout, "randread: host=%s users=%d concurrency=%d duration=%s target=%.2f/s\n",
		host, numUsers, flagConcurrency, flagDuration, rl.Rate()*float64(numUsers))

	var wg sync.WaitGroup

	for i := range boxes {
		mb := boxes[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			randreadUserSession(ctx, cfg, mb, rl, m, flagConcurrency)
		}()
	}

	// Progress ticker
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		prev := bench.ProgressState{Time: time.Now()}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prev = m.PrintProgress(os.Stdout, prev)
			}
		}
	}()

	wg.Wait()
	m.PrintSummary(os.Stdout)
	return nil
}

func randreadUserSession(
	ctx context.Context,
	cfg connConfig,
	mb mailbox.Mailbox,
	rl *bench.RateLimiter,
	m *bench.Metrics,
	concurrency int,
) {
	client, selectData, err := connectAndLogin(cfg, mb)
	if err != nil {
		log.Printf("connect error for %s: %v", mb.Username, err)
		return
	}
	defer closeClient(client)

	if selectData.NumMessages == 0 {
		log.Printf("INBOX empty for %s, skipping", mb.Username)
		return
	}

	exists := selectData.NumMessages

	var innerWg sync.WaitGroup
	for i := range concurrency {
		seed := time.Now().UnixNano() + int64(i)
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			randreadLoop(ctx, client, exists, rl, m, cfg.cmdTimeout, seed)
		}()
	}
	innerWg.Wait()
}

func randreadLoop(
	ctx context.Context,
	client *imapclient.Client,
	exists uint32,
	rl *bench.RateLimiter,
	m *bench.Metrics,
	cmdTimeout time.Duration,
	seed int64,
) {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rl.Wait()

		select {
		case <-ctx.Done():
			return
		default:
		}

		seqNum := uint32(rng.Int63n(int64(exists))) + 1

		var seqSet imap.SeqSet
		seqSet.AddNum(seqNum)

		fetchOpts := &imap.FetchOptions{
			BodySection: []*imap.FetchItemBodySection{{Peek: true}},
		}

		type randResult struct {
			bytes int64
			err   error
		}
		done := make(chan randResult, 1)
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
			done <- randResult{bytes: b, err: fetchCmd.Close()}
		}()

		select {
		case res := <-done:
			if res.err != nil {
				log.Printf("fetch error (randread seq=%d): %v", seqNum, res.err)
				return
			}
			m.RecordOp(time.Since(t0), 1, res.bytes)
		case <-time.After(cmdTimeout):
			log.Printf("fetch timeout (randread seq=%d)", seqNum)
			return
		case <-ctx.Done():
			return
		}
	}
}
