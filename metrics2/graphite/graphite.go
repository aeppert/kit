// Package graphite provides a Graphite implementation for metrics. Metrics are
// emitted with each observation in the plaintext protocol, which looks like
//
//   "<metric path> <metric value> <metric timestamp>"
//
// http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol
package graphite

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics2/generic"
)

// Graphite is a collection of individual metrics. All metrics are updated
// in-place, and can be snapshotted and written to a writer with
// WriteTo/FlushTo.
//
// To use this in your service, create a Graphite object, create all of the
// metrics you want from it, and then tell the invoke FlushTo against a
// util/conn.Manager.
//
//   g := graphite.New()
//   go g.FlushTo(conn.NewManager(...), time.NewTicker(5*time.Second))
//
type Graphite struct {
	mtx        sync.RWMutex
	prefix     string
	counters   map[string]*generic.Counter
	gauges     map[string]*generic.Gauge
	histograms map[string]*generic.SimpleHistogram
	logger     log.Logger
}

// New returns a Graphite object capable of allocating individual metrics. All
// metrics will share the given prefix in their path. All metrics can be
// snapshotted, and their values and statistical summaries written to a writer,
// via the WriteTo method.
func New(prefix string, logger log.Logger) *Graphite {
	return &Graphite{
		prefix:     prefix,
		counters:   make(map[string]*generic.Counter),
		gauges:     make(map[string]*generic.Gauge),
		histograms: make(map[string]*generic.SimpleHistogram),
		logger:     logger,
	}
}

// NewCounter allocates and returns a counter with the given name.
func (g *Graphite) NewCounter(name string) *generic.Counter {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	c := generic.NewCounter()
	g.counters[g.prefix+name] = c
	return c
}

// NewGauge allocates and returns a gauge with the given name.
func (g *Graphite) NewGauge(name string) *generic.Gauge {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	ga := generic.NewGauge()
	g.gauges[g.prefix+name] = ga
	return ga
}

// NewHistogram allocates and returns a simple histogram with the given name.
func (g *Graphite) NewHistogram(name string) *generic.SimpleHistogram {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	h := generic.NewSimpleHistogram()
	g.histograms[g.prefix+name] = h
	return h
}

// FlushTo flushes the contents to the writer every time the ticker fires.
// FlushTo blocks until the ticker is stopped.
func (g *Graphite) FlushTo(w io.Writer, ticker *time.Ticker) {
	for range ticker.C {
		if _, err := g.WriteTo(w); err != nil {
			g.logger.Log("during", "Flush", "err", err)
		}
	}
}

// WriteTo writes a snapshot of all of the allocated metrics to the writer in
// the Graphite simple text format.
func (g *Graphite) WriteTo(w io.Writer) (int64, error) {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	var (
		n     int
		count int64
		err   error
		now   = time.Now().Unix()
	)
	for path, c := range g.counters {
		n, err = fmt.Fprintf(w, "%s.count %f %d\n", path, c.Value(), now)
		if err != nil {
			return count, err
		}
		count += int64(n)
	}
	for path, ga := range g.gauges {
		n, err = fmt.Fprintf(w, "%s %f %d\n", path, ga.Value(), now)
		if err != nil {
			return count, err
		}
		count += int64(n)
	}
	for path, h := range g.histograms {
		n, err = fmt.Fprintf(w, "%s.mean %f %d\n", path, h.ApproximateMovingAverage(), now)
		if err != nil {
			return count, err
		}
		count += int64(n)
	}
	return count, nil
}
