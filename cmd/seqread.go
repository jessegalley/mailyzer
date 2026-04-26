package cmd

import (
	"context"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/jessegalley/mailyzer/internal/bench"
	"github.com/jessegalley/mailyzer/internal/mailbox"
	"github.com/spf13/cobra"
)

var flagPageSize int

var seqreadCmd = &cobra.Command{
	Use:   "seqread [flags] <host>",
	Short: "Sequential page read benchmark",
	Args:  cobra.ExactArgs(1),
	RunE:  runSeqread,
}

func init() {
	seqreadCmd.Flags().IntVar(&flagPageSize, "page-size", 10, "messages per page")
	rootCmd.AddCommand(seqreadCmd)
}

// pageState tracks sequential page assignments across C goroutines on one connection.
type pageState struct {
	counter  atomic.Int64
	exists   uint32
	pageSize int
}

// nextRange returns the [startSeq, endSeq] for the next page, wrapping around.
func (ps *pageState) nextRange() (start, end uint32) {
	numPages := int64(ps.exists) / int64(ps.pageSize)
	if numPages == 0 {
		numPages = 1
	}

	page := ps.counter.Add(1) - 1
	page = page % numPages

	end = min(ps.exists-uint32(page)*uint32(ps.pageSize), ps.exists)
	if uint32(ps.pageSize) > end {
		start = 1
	} else {
		start = end - uint32(ps.pageSize) + 1
	}
	if start < 1 {
		start = 1
	}
	return start, end
}

func runSeqread(cmd *cobra.Command, args []string) error {
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
	m := bench.NewMetrics(errWriter)
	ctx, cancel := context.WithTimeout(context.Background(), flagDuration)
	defer cancel()

	log.Printf("seqread: host=%s users=%d concurrency=%d page-size=%d fetch=%s duration=%s",
		host, len(boxes), flagConcurrency, flagPageSize, flagFetchMode, flagDuration)

	var wg sync.WaitGroup
	for i := range boxes {
		mb := boxes[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			seqreadUserSession(ctx, cfg, mb, m, flagConcurrency, flagPageSize, fetchOpts)
		}()
	}

	go progressTicker(ctx, m)

	wg.Wait()
	m.PrintSummary(os.Stdout)
	return nil
}

func seqreadUserSession(
	ctx context.Context,
	cfg connConfig,
	mb mailbox.Mailbox,
	m *bench.Metrics,
	concurrency, pageSize int,
	fetchOpts *imap.FetchOptions,
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

	ps := &pageState{
		exists:   selectData.NumMessages,
		pageSize: pageSize,
	}

	var innerWg sync.WaitGroup
	for range concurrency {
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			seqreadLoop(ctx, client, ps, m, cfg.cmdTimeout, fetchOpts)
		}()
	}
	innerWg.Wait()
}

type fetchResult struct {
	n     int64
	bytes int64
	err   error
}

func seqreadLoop(
	ctx context.Context,
	client *imapclient.Client,
	ps *pageState,
	m *bench.Metrics,
	cmdTimeout time.Duration,
	fetchOpts *imap.FetchOptions,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		startSeq, endSeq := ps.nextRange()

		var seqSet imap.SeqSet
		seqSet.AddRange(startSeq, endSeq)

		done := make(chan fetchResult, 1)
		t0 := time.Now()

		go func() {
			fetchCmd := client.Fetch(seqSet, fetchOpts)
			var n, b int64
			for {
				msg := fetchCmd.Next()
				if msg == nil {
					break
				}
				b += drainMessage(msg)
				n++
			}
			err := fetchCmd.Close()
			done <- fetchResult{n: n, bytes: b, err: err}
		}()

		select {
		case res := <-done:
			if res.err != nil {
				m.RecordError("fetch error (seqread %d:%d): %v", startSeq, endSeq, res.err)
				return
			}
			m.RecordOp(time.Since(t0), res.n, res.bytes)
		case <-time.After(cmdTimeout):
			m.RecordError("fetch timeout (seqread %d:%d)", startSeq, endSeq)
			return
		case <-ctx.Done():
			return
		}
	}
}

// drainMessage consumes all fetch item data from a message, discarding body
// literals, and returns the total bytes read from body sections.
func drainMessage(msg *imapclient.FetchMessageData) int64 {
	var total int64
	for {
		item := msg.Next()
		if item == nil {
			break
		}
		if bs, ok := item.(imapclient.FetchItemDataBodySection); ok {
			n, _ := io.Copy(io.Discard, bs.Literal)
			total += n
		}
	}
	return total
}
