package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/jessegalley/mailyzer/internal/bench"
	"github.com/jessegalley/mailyzer/internal/mailbox"
	"github.com/spf13/cobra"
)

var (
	flagUser     string
	flagPassword string
	flagFile     string

	flagPort          int
	flagTLSSkipVerify bool
	flagNoTLS         bool

	flagDuration    time.Duration
	flagConcurrency int
	flagTimeout     time.Duration
	flagFetchMode   string
	flagMailbox     string
	flagErrLog      string
)

var rootCmd = &cobra.Command{
	Use:   "mailyzer",
	Short: "IMAP server I/O benchmark tool",
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	pf := rootCmd.PersistentFlags()

	pf.StringVarP(&flagUser, "user", "u", "", "IMAP username")
	pf.StringVarP(&flagPassword, "password", "p", "", "IMAP password")
	pf.StringVarP(&flagFile, "file", "f", "", "mailbox file (whitespace-separated: username password)")

	pf.IntVar(&flagPort, "port", 993, "IMAP server port")
	pf.BoolVar(&flagTLSSkipVerify, "tls-skip-verify", false, "skip TLS certificate verification")
	pf.BoolVar(&flagNoTLS, "no-tls", false, "use plain TCP (no TLS)")

	pf.DurationVarP(&flagDuration, "duration", "d", 60*time.Second, "benchmark duration")
	pf.IntVar(&flagConcurrency, "concurrency", 1, "goroutines per IMAP connection")
	pf.DurationVar(&flagTimeout, "timeout", 30*time.Second, "per-command IMAP timeout")
	pf.StringVar(&flagFetchMode, "fetch-mode", "all", "what to fetch per message: all, body, headers, envelope")
	pf.StringVarP(&flagMailbox, "mailbox", "m", "INBOX", "mailbox to use for all operations")
	pf.StringVar(&flagErrLog, "errlog", "", "write per-operation errors to this file (default: silent)")
}

// openErrLog opens flagErrLog for appending and returns the writer and a close
// function. If flagErrLog is empty the writer is nil (errors are counted but
// not logged). The caller must call close() when done.
func openErrLog() (io.Writer, func()) {
	if flagErrLog == "" {
		return nil, func() {}
	}
	f, err := os.OpenFile(flagErrLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("warning: cannot open errlog %q: %v (errors will be silent)", flagErrLog, err)
		return nil, func() {}
	}
	return f, func() { f.Close() }
}

type connConfig struct {
	host          string
	port          int
	noTLS         bool
	tlsSkipVerify bool
	cmdTimeout    time.Duration
}

func newConnConfig(host string) connConfig {
	return connConfig{
		host:          host,
		port:          flagPort,
		noTLS:         flagNoTLS,
		tlsSkipVerify: flagTLSSkipVerify,
		cmdTimeout:    flagTimeout,
	}
}

// buildFetchOptions returns the FetchOptions for the given mode:
//
//	all      — BODY.PEEK[]             full message bytes (headers + body)
//	body     — BODY.PEEK[TEXT]         body only, no headers
//	headers  — BODY.PEEK[HEADER]       raw header block only
//	envelope — ENVELOPE FLAGS RFC822.SIZE  parsed index fields, no body I/O
func buildFetchOptions(mode string) (*imap.FetchOptions, error) {
	switch mode {
	case "all":
		return &imap.FetchOptions{
			BodySection: []*imap.FetchItemBodySection{{Peek: true}},
		}, nil
	case "body":
		return &imap.FetchOptions{
			BodySection: []*imap.FetchItemBodySection{{
				Peek:      true,
				Specifier: imap.PartSpecifierText,
			}},
		}, nil
	case "headers":
		return &imap.FetchOptions{
			BodySection: []*imap.FetchItemBodySection{{
				Peek:      true,
				Specifier: imap.PartSpecifierHeader,
			}},
		}, nil
	case "envelope":
		return &imap.FetchOptions{
			Envelope:   true,
			Flags:      true,
			RFC822Size: true,
		}, nil
	default:
		return nil, fmt.Errorf("unknown fetch-mode %q: must be one of: all, body, headers, envelope", mode)
	}
}

// dialAndLogin dials the server and authenticates. It does NOT select a mailbox.
// Used by the append command which doesn't need a selected mailbox.
func dialAndLogin(cfg connConfig, mb mailbox.Mailbox) (*imapclient.Client, error) {
	address := fmt.Sprintf("%s:%d", cfg.host, cfg.port)

	var (
		client *imapclient.Client
		err    error
	)

	if cfg.noTLS {
		client, err = imapclient.DialInsecure(address, nil)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", address, err)
		}
	} else {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.tlsSkipVerify, //nolint:gosec
		}
		client, err = imapclient.DialTLS(address, &imapclient.Options{TLSConfig: tlsCfg})
		if err != nil {
			return nil, fmt.Errorf("dial TLS %s: %w", address, err)
		}
	}

	if err := client.WaitGreeting(); err != nil {
		client.Close()
		return nil, fmt.Errorf("greeting from %s: %w", cfg.host, err)
	}

	if err := client.Login(mb.Username, mb.Password).Wait(); err != nil {
		client.Close()
		return nil, fmt.Errorf("login %s: %w", mb.Username, err)
	}

	return client, nil
}

// connectAndLogin dials the server, authenticates, and selects the configured mailbox.
// The caller must call closeClient when done.
func connectAndLogin(cfg connConfig, mb mailbox.Mailbox) (*imapclient.Client, *imap.SelectData, error) {
	client, err := dialAndLogin(cfg, mb)
	if err != nil {
		return nil, nil, err
	}

	selectData, err := client.Select(flagMailbox, nil).Wait()
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("SELECT %s for %s: %w", flagMailbox, mb.Username, err)
	}

	return client, selectData, nil
}

// closeClient closes the IMAP connection. We skip the graceful LOGOUT because
// on a broken server Logout().Wait() blocks indefinitely, which would stall
// the benchmark shutdown. Closing the TCP connection is sufficient — the server
// will clean up the session, and any goroutine blocked inside a command will
// get an immediate error from the closed connection.
func closeClient(c *imapclient.Client) {
	_ = c.Close()
}

// progressTicker prints a progress line every 5 seconds until ctx is done.
func progressTicker(ctx context.Context, m *bench.Metrics) {
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
}
