package tso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// DefaultClientTimeout bounds a single attempt against one oracle member.
const DefaultClientTimeout = 500 * time.Millisecond

// Client is a transaction timestamp oracle that talks to a replicated tso
// group over HTTP. It implements txn.Oracle.
//
// The client keeps no state that affects correctness. All it does is find the
// group's leader and ask it for a timestamp; the leadership check, the window
// allocation and the monotonicity guarantee all live on the server side.
type Client struct {
	// addrs is indexed by member id, so a "not leader" routing hint (which is a
	// member id) is directly usable.
	addrs []string

	// assumedLeader is the member to try first.
	assumedLeader int

	timeout time.Duration
	http    *http.Client

	mu sync.Mutex
}

// NewClient creates a timestamp client for the given oracle members.
func NewClient(addrs []string) *Client {
	return &Client{
		addrs:   addrs,
		timeout: DefaultClientTimeout,
		http:    &http.Client{},
	}
}

// SetTimeout overrides the per-attempt timeout.
func (c *Client) SetTimeout(d time.Duration) {
	if d > 0 {
		c.timeout = d
	}
}

// ReadTS implements txn.Oracle: it returns the snapshot timestamp for a new
// transaction.
func (c *Client) ReadTS(ctx context.Context) (txn.Timestamp, error) {
	ts, err := c.next(ctx)
	return txn.Timestamp(ts), err
}

// CommitTS implements txn.Oracle. The server guarantees the value it returns
// exceeds every previously issued timestamp, and this method additionally
// verifies the ordering the protocol requires, so a misbehaving or
// misconfigured oracle is caught here rather than corrupting a snapshot.
func (c *Client) CommitTS(ctx context.Context, startTS txn.Timestamp) (txn.Timestamp, error) {
	ts, err := c.next(ctx)
	if err != nil {
		return 0, err
	}
	if txn.Timestamp(ts) <= startTS {
		return 0, fmt.Errorf("tso: commit timestamp %d does not exceed start timestamp %d",
			ts, startTS)
	}
	return txn.Timestamp(ts), nil
}

// Close implements txn.Oracle.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// next asks the oracle group for one timestamp, following leadership.
func (c *Client) next(ctx context.Context) (uint64, error) {
	if len(c.addrs) == 0 {
		return 0, fmt.Errorf("tso: client has no member addresses")
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = DefaultClientTimeout
	}

	// missed counts consecutive attempts with no usable leader hint, so an oracle
	// group that is electing is retried politely instead of hammered.
	missed := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		idx := c.leaderIndex()
		url := fmt.Sprintf("http://%s/tso/next/", c.addrs[idx])

		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := c.post(attemptCtx, url, NextRequest{Count: 1})
		cancel()

		if err == nil && resp.Status == "ok" {
			c.setLeader(idx)
			return resp.Start, nil
		}

		if err == nil && resp.Status == "not-leader" {
			if resp.LeaderID >= 0 && resp.LeaderID < len(c.addrs) {
				// The member named the leader: aim the next attempt at it.
				c.setLeader(resp.LeaderID)
				missed = 0
				continue
			}
		}

		// The member was unreachable, or disclaimed leadership without naming a
		// successor. Sweep the members, then pause.
		c.rotate()
		if missed++; missed >= len(c.addrs) {
			missed = 0
			if err := sleepCtx(ctx, 20*time.Millisecond); err != nil {
				return 0, err
			}
		}
	}
}

func (c *Client) post(ctx context.Context, url string, reqData any) (NextResponse, error) {
	var out NextResponse
	body := new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(reqData); err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("tso: %s: HTTP %d: %s", url, resp.StatusCode, string(raw))
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("tso: decoding response from %s: %w", url, err)
	}
	return out, nil
}

func (c *Client) leaderIndex() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.assumedLeader < 0 || c.assumedLeader >= len(c.addrs) {
		c.assumedLeader = 0
	}
	return c.assumedLeader
}

func (c *Client) setLeader(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assumedLeader = idx
}

func (c *Client) rotate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assumedLeader = (c.assumedLeader + 1) % len(c.addrs)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var _ txn.Oracle = (*Client)(nil)
