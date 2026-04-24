package mailbox

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Mailbox holds a single IMAP credential pair.
type Mailbox struct {
	Username string
	Password string
}

// Load resolves credentials from a file path or explicit flags.
// If file is non-empty, LoadFromFile is used; otherwise LoadFromFlags.
func Load(file, user, password string) ([]Mailbox, error) {
	if file != "" {
		return LoadFromFile(file)
	}
	return LoadFromFlags(user, password)
}

// LoadFromFile reads a whitespace-delimited file where each line is "username password".
// Blank lines and lines starting with '#' are skipped.
func LoadFromFile(path string) ([]Mailbox, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mailbox file: %w", err)
	}
	defer f.Close()

	var boxes []Mailbox
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("mailbox file line %d: expected 2 fields (username password), got %d", lineNum, len(fields))
		}
		boxes = append(boxes, Mailbox{Username: fields[0], Password: fields[1]})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading mailbox file: %w", err)
	}
	if len(boxes) == 0 {
		return nil, fmt.Errorf("mailbox file %q contains no entries", path)
	}
	return boxes, nil
}

// LoadFromFlags returns a single-element slice from explicit user/password flags.
func LoadFromFlags(user, password string) ([]Mailbox, error) {
	if user == "" {
		return nil, fmt.Errorf("must provide -u/--user or -f/--file")
	}
	if password == "" {
		return nil, fmt.Errorf("must provide -p/--password or -f/--file")
	}
	return []Mailbox{{Username: user, Password: password}}, nil
}
