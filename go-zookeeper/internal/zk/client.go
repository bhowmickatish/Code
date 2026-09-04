package zk

import (
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

type Client struct {
	conn    *zk.Conn
	events  <-chan zk.Event
	openACL bool
	digest  string
}

func Connect(addrs []string, sessionTimeout time.Duration, openACL bool, digest string) (*Client, error) {
	conn, events, err := zk.Connect(addrs, sessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("connect zookeeper: %w", err)
	}

	if digest != "" {
		if err := conn.AddAuth("digest", []byte(digest)); err != nil {
			conn.Close()
			return nil, fmt.Errorf("zookeeper digest auth: %w", err)
		}
	}

	return &Client{
		conn:    conn,
		events:  events,
		openACL: openACL,
		digest:  digest,
	}, nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) Events() <-chan zk.Event {
	return c.events
}

func (c *Client) Exists(path string) (bool, error) {
	exists, _, err := c.conn.Exists(path)
	if err != nil {
		return false, fmt.Errorf("exists %q: %w", path, err)
	}
	return exists, nil
}

func (c *Client) Get(path string) ([]byte, error) {
	data, _, err := c.conn.Get(path)
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", path, err)
	}
	return data, nil
}

func (c *Client) Set(path string, data []byte) error {
	_, err := c.conn.Set(path, data, -1)
	if err != nil {
		return fmt.Errorf("set %q: %w", path, err)
	}
	return nil
}

func (c *Client) CreatePath(path string, data []byte) error {
	parts := splitPath(path)
	if len(parts) == 0 {
		return fmt.Errorf("invalid path %q", path)
	}

	current := ""
	for i, part := range parts {
		current += "/" + part
		exists, _, err := c.conn.Exists(current)
		if err != nil {
			return fmt.Errorf("exists %q: %w", current, err)
		}
		if exists {
			continue
		}

		var payload []byte
		if i == len(parts)-1 {
			payload = data
		}
		_, err = c.conn.Create(current, payload, 0, c.createACL())
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("create %q: %w", current, err)
		}
	}
	return nil
}

func (c *Client) createACL() []zk.ACL {
	if c.openACL {
		return zk.WorldACL(zk.PermAll)
	}
	if c.digest != "" {
		user, password, ok := splitDigest(c.digest)
		if ok {
			return zk.DigestACL(zk.PermAll, user, password)
		}
	}
	return zk.WorldACL(zk.PermRead | zk.PermWrite | zk.PermCreate | zk.PermDelete)
}

func splitDigest(digest string) (user, password string, ok bool) {
	for i := 0; i < len(digest); i++ {
		if digest[i] == ':' {
			return digest[:i], digest[i+1:], true
		}
	}
	return "", "", false
}

func (c *Client) Watch(path string) (<-chan zk.Event, error) {
	_, _, ch, err := c.conn.GetW(path)
	if err != nil {
		return nil, fmt.Errorf("watch %q: %w", path, err)
	}
	return ch, nil
}

func splitPath(path string) []string {
	path = trimLeadingSlash(path)
	if path == "" {
		return nil
	}
	parts := make([]string, 0, 4)
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '/' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	return parts
}

func trimLeadingSlash(path string) string {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return path
}
