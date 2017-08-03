package pool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.Mutex
	conns chan *PoolConn

	// net.Conn generator
	factory Factory

	// idleConnTimeout is the max amout of time an idle(keep-alive) conn will
	// remain idle before closing itself.
	// Zero means no limit.
	idleConnTimeout time.Duration
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewChannelPool returns a new pool based on buffered channels with an initial
// capacity and maximum capacity. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool, a new connection will be created via the Factory()
// method.
func NewChannelPool(initialCap, maxCap int, maxIdleTime time.Duration, factory Factory) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		conns:   make(chan *PoolConn, maxCap),
		factory: factory,
	}
	if maxIdleTime > 0 {
		c.idleConnTimeout = maxIdleTime
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCap; i++ {
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		p := &PoolConn{
			c: c,
		}
		p.Conn = conn
		if maxIdleTime > 0 {
			p.idleAt = time.Now()
		}

		c.conns <- p
	}

	return c, nil
}

func (c *channelPool) getConns() chan *PoolConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get() (net.Conn, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	for {
		select {
		case pconn := <-conns:
			if pconn.Conn == nil {
				return nil, ErrClosed
			}

			if timeout := c.idleConnTimeout; timeout > 0 {
				if pconn.idleAt.Add(timeout).Before(time.Now()) {
					pconn.MarkUnusable()
					pconn.Close()
					continue
				}
			}

			return pconn, nil
		default:
			conn, err := c.factory()
			if err != nil {
				return nil, err
			}
			p := &PoolConn{
				c: c,
			}
			p.Conn = conn
			if c.idleConnTimeout > 0 {
				p.idleAt = time.Now()
			}

			return p, nil
		}

	}
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(p *PoolConn) error {
	if p.Conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return p.Conn.Close()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if c.idleConnTimeout > 0 {
		p.idleAt = time.Now()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- p:
		return nil
	default:
		// pool is full, close passed connection
		return p.Conn.Close()
	}
}

func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Conn.Close()
	}
}

func (c *channelPool) Len() int { return len(c.getConns()) }
