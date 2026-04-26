package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/user"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/jessegalley/mailyzer/internal/bench"
	"github.com/jessegalley/mailyzer/internal/mailbox"
	"github.com/spf13/cobra"
)

var (
	flagMinSize int
	flagMaxSize int
)

// appendSeq is a global counter ensuring every appended message gets a unique
// number regardless of which user or goroutine generates it.
var appendSeq atomic.Int64

const hexPoolSize = 64 * 1024 * 1024 // 64 MiB

var appendCmd = &cobra.Command{
	Use:   "append [flags] <host>",
	Short: "Write benchmark: continuously append RFC-compliant messages",
	Args:  cobra.ExactArgs(1),
	RunE:  runAppend,
}

func init() {
	appendCmd.Flags().IntVar(&flagMinSize, "min-size", 256, "minimum message body size in bytes")
	appendCmd.Flags().IntVar(&flagMaxSize, "max-size", 131072, "maximum message body size in bytes (default 128k)")
	rootCmd.AddCommand(appendCmd)
}

func runAppend(cmd *cobra.Command, args []string) error {
	if flagMinSize > flagMaxSize {
		return fmt.Errorf("--min-size (%d) must be <= --max-size (%d)", flagMinSize, flagMaxSize)
	}

	host := args[0]
	cfg := newConnConfig(host)

	boxes, err := mailbox.Load(flagFile, flagUser, flagPassword)
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

	log.Printf("append: host=%s users=%d concurrency=%d mailbox=%s min=%d max=%d duration=%s from=%s",
		host, len(boxes), flagConcurrency, flagMailbox, flagMinSize, flagMaxSize, flagDuration, from)

	var wg sync.WaitGroup
	for i := range boxes {
		mb := boxes[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			appendUserSession(ctx, cfg, mb, from, pool, m, flagConcurrency, flagMinSize, flagMaxSize)
		}()
	}

	go progressTicker(ctx, m)
	wg.Wait()
	m.PrintSummary(os.Stdout)
	return nil
}

func appendUserSession(
	ctx context.Context,
	cfg connConfig,
	mb mailbox.Mailbox,
	from string,
	pool []byte,
	m *bench.Metrics,
	concurrency, minSize, maxSize int,
) {
	client, err := dialAndLogin(cfg, mb)
	if err != nil {
		log.Printf("connect error for %s: %v", mb.Username, err)
		return
	}
	defer closeClient(client)

	var innerWg sync.WaitGroup
	for i := range concurrency {
		seed := time.Now().UnixNano() + int64(i)
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			appendLoop(ctx, client, mb.Username, from, pool, m, cfg.cmdTimeout, minSize, maxSize, seed)
		}()
	}
	innerWg.Wait()
}

func appendLoop(
	ctx context.Context,
	client *imapclient.Client,
	toAddr, from string,
	pool []byte,
	m *bench.Metrics,
	cmdTimeout time.Duration,
	minSize, maxSize int,
	seed int64,
) {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		seq := appendSeq.Add(1)
		msgBytes := buildMessage(seq, toAddr, from, pool, minSize, maxSize, rng)

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
				m.RecordError("append error (seq=%d): %v", seq, res.err)
				return
			}
			m.RecordOp(time.Since(t0), 1, int64(len(msgBytes)))
		case <-time.After(cmdTimeout):
			m.RecordError("append timeout (seq=%d)", seq)
			return
		case <-ctx.Done():
			return
		}
	}
}

// buildMessage constructs a minimal RFC 5322 message. The body is drawn from
// pool at a random offset (wrapping around), prefixed with the sequence number
// to prevent server-side deduplication or caching.
func buildMessage(seq int64, toAddr, from string, pool []byte, minSize, maxSize int, rng *rand.Rand) []byte {
	bodyLen := minSize
	if maxSize > minSize {
		bodyLen = minSize + rng.Intn(maxSize-minSize+1)
	}
	bodyLen = min(bodyLen, len(pool))

	offset := rng.Intn(len(pool))
	var body []byte
	if offset+bodyLen <= len(pool) {
		body = pool[offset : offset+bodyLen]
	} else {
		part1 := pool[offset:]
		needed := bodyLen - len(part1)
		body = make([]byte, 0, bodyLen)
		body = append(body, part1...)
		body = append(body, pool[:needed]...)
	}

	now := time.Now()
	hostname := msgHostname()

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "From: %s\r\n", from)
	fmt.Fprintf(&buf, "To: %s\r\n", toAddr)
	fmt.Fprintf(&buf, "Subject: [mailyzer] test message #%d\r\n", seq)
	fmt.Fprintf(&buf, "Date: %s\r\n", now.Format(time.RFC1123Z))
	fmt.Fprintf(&buf, "Message-ID: <%d.%d@%s>\r\n", seq, now.UnixNano(), hostname)
	fmt.Fprintf(&buf, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&buf, "Content-Type: text/plain; charset=us-ascii\r\n")
	fmt.Fprintf(&buf, "\r\n")
	fmt.Fprintf(&buf, "x-mailyzer-seq: %d\r\n\r\n", seq)
	buf.Write(body)

	return buf.Bytes()
}

// generateHexPool creates a 64 MiB pool of printable hex characters using
// math/rand. Called once at benchmark start; goroutines read from it concurrently
// (read-only after generation, no locking needed).
func generateHexPool() []byte {
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	raw := make([]byte, hexPoolSize/2)
	for i := range raw {
		raw[i] = byte(rng.Intn(256))
	}
	pool := make([]byte, hexPoolSize)
	hex.Encode(pool, raw)
	return pool
}

// senderAddr returns "linuxuser@hostname" for the From header, ensuring the
// hostname is an FQDN by appending ".tld" if it contains no dots.
func senderAddr() string {
	username := "mailyzer"
	if u, err := user.Current(); err == nil && u.Username != "" {
		username = u.Username
	}
	return fmt.Sprintf("%s@%s", username, msgHostname())
}

// msgHostname returns the local hostname, guaranteed to contain at least one dot.
func msgHostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		h = "localhost"
	}
	if !strings.Contains(h, ".") {
		h += ".tld"
	}
	return h
}
