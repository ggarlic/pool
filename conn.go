package pool

import (
	"net"
	"sync"
	"time"
)

// PoolConn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type PoolConn struct {
	net.Conn
	mu       sync.RWMutex
	c        *channelPool
	idleAt   time.Time
	unusable bool
}

// Close() puts the given connects back to the pool instead of closing it.
func (p *PoolConn) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		if p.Conn != nil {
			return p.Conn.Close()
		}
		return nil
	}
	// TODO add timeout check
	return p.c.put(p)
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *PoolConn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

//// newConn wraps a standard net.Conn to a poolConn net.Conn.
//func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
//p := &PoolConn{
//c:      c,
//idleAt: time.Now(),
//}
//p.Conn = conn
//return p
//}
