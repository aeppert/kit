// Package emitter provides a helper for push metrics. The emitter receives and
// buffers metrics, and emits them to the remote on a regular basis.
package emitter

import (
	"bytes"
	"fmt"
	"time"

	"container/ring"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/util/conn"
)

// Emitter receives data from push-based metrics, stores them in a ring buffer,
// and publishes to the remote collector on regular intervals.
type Emitter struct {
	incomingc chan string
	buffer    *ring.Ring
	manager   *conn.Manager
	logger    log.Logger
	quitc     chan chan struct{}
}

// TickerFunc imitates time.NewTicker.
type TickerFunc func(time.Duration) *time.Ticker

func NewEmitter(
	dialer conn.Dialer,
	network, address string,
	after conn.AfterFunc,
	ticker TickerFunc,
	prefix string,
	flushInterval time.Duration,
	flushSize int,
	maxBufferSize int,
	logger log.Logger,
) *Emitter {
	if maxBufferSize < flushSize {
		maxBufferSize = flushSize
	}
	e := &Emitter{
		incomingc: make(chan string),
		buffer:    nil, // initially empty
		manager:   conn.NewManager(dialer, network, address, after, logger),
		logger:    logger,
		quitc:     make(chan chan struct{}),
	}
	go e.loop(ticker, flushInterval, flushSize)
	return e
}

func (e *Emitter) loop(ticker TickerFunc, flushInterval time.Duration, flushSize int) {
	tk := ticker(flushInterval)
	defer tk.Stop()
	buf := &bytes.Buffer{}
	for {
		select {
		case <-tk.C:
			e.Flush(buf)

		case s := <-e.incomingc:
			node := &ring.Ring{Value: s}
			e.buffer.Link(node)
			fmt.Fprintf(buf, s)
			if buf.Len() >= flushSize {
				e.Flush(buf)
			}

		case q := <-e.quitc:
			e.Flush(buf)
			close(q)
			return
		}
	}
}

// Stop flushes any buffered data and terminates the emitter.
func (e *Emitter) Stop() {
	q := make(chan struct{})
	e.quitc <- q
	<-q
}

// Flush writes the buffer to the connection.
// If the connection is unavailable, the data will continue to aggregate.
func (e *Emitter) Flush(buf *bytes.Buffer) {
	conn := e.manager.Take()
	if conn == nil {
		e.logger.Log("during", "Flush", "err", "connection unavailable")
		return
	}
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		e.logger.Log("during", "Flush", "err", err)
	}
	buf.Reset()
	e.manager.Put(err)
}
