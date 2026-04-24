package cmd

import (
	"crypto/tls"
	"fmt"
	"time"

	imap "github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
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
	flagHalfLife    time.Duration
	flagThreshold   float64
	flagNoLimit     bool
	flagFetchMode   string
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
	pf.DurationVar(&flagHalfLife, "half-life", 1*time.Second, "rate limiter half-life")
	pf.Float64Var(&flagThreshold, "threshold", 150.0, "rate limiter threshold (rate = ln2/halfLife * threshold)")
	pf.BoolVar(&flagNoLimit, "no-limit", false, "disable rate limiting (run as fast as possible)")
	pf.StringVar(&flagFetchMode, "fetch-mode", "all", "what to fetch per message: all, body, headers, envelope")
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

// connectAndLogin dials the IMAP server, logs in with the given credentials,
// and selects INBOX. The caller must call closeClient when done.
func connectAndLogin(cfg connConfig, mb mailbox.Mailbox) (*imapclient.Client, *imap.SelectData, error) {
	address := fmt.Sprintf("%s:%d", cfg.host, cfg.port)

	var client *imapclient.Client
	var err error

	if cfg.noTLS {
		client, err = imapclient.DialInsecure(address, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("dial %s: %w", address, err)
		}
	} else {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.tlsSkipVerify, //nolint:gosec
		}
		opts := &imapclient.Options{TLSConfig: tlsCfg}
		client, err = imapclient.DialTLS(address, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("dial TLS %s: %w", address, err)
		}
	}

	if err := client.WaitGreeting(); err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("greeting from %s: %w", cfg.host, err)
	}

	if err := client.Login(mb.Username, mb.Password).Wait(); err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("login %s: %w", mb.Username, err)
	}

	selectData, err := client.Select("INBOX", nil).Wait()
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("SELECT INBOX for %s: %w", mb.Username, err)
	}

	return client, selectData, nil
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

// closeClient logs out and closes the IMAP client connection.
func closeClient(c *imapclient.Client) {
	_ = c.Logout().Wait()
	_ = c.Close()
}
