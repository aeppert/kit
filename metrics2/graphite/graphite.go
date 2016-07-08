// Package graphite provides a Graphite implementation for metrics. Metrics are
// emitted with each observation in the plaintext protocol. See
// http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol
// for more information.
//
// Graphite does not have a native understanding of metric parameterization, so
// label values are aggregated but not reported. Use distinct metrics for each
// unique combination of label values.
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
	mtx         sync.RWMutex
	prefix      string
	counters    map[string]*generic.Counter
	gauges      map[string]*generic.Gauge
	shistograms map[string]*generic.SimpleHistogram
	histograms  map[string]*generic.Histogram
	logger      log.Logger
}

// New returns a Graphite object capable of allocating individual metrics. All
// metrics will share the given prefix in their path. All metrics can be
// snapshotted, and their values and statistical summaries written to a writer,
// via the WriteTo method.
func New(prefix string, logger log.Logger) *Graphite {
	return &Graphite{
		prefix:      prefix,
		counters:    map[string]*generic.Counter{},
		gauges:      map[string]*generic.Gauge{},
		shistograms: map[string]*generic.SimpleHistogram{},
		histograms:  map[string]*generic.Histogram{},
		logger:      logger,
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

// NewSimpleHistogram allocates and returns a simple histogram with the given
// name. Simple histograms report their approximate moving average value in a
// metric with the .mean suffix.
func (g *Graphite) NewSimpleHistogram(name string) *generic.SimpleHistogram {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	h := generic.NewSimpleHistogram()
	g.shistograms[g.prefix+name] = h
	return h
}

// NewHistogram allocates and returns a histogram with the given name and bucket
// count. 50 is a good default number of buckets. Histograms report their 50th,
// 90th, 95th, and 99th quantiles in distinct metrics with the .p50, .p90, .p95,
// and .p99 suffixes, respectively.
func (g *Graphite) NewHistogram(name string, buckets int) *generic.Histogram {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	h := generic.NewHistogram(buckets)
	g.histograms[g.prefix+name] = h
	return h
}

// FlushTo invokes WriteTo to the writer every time the ticker fires.
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
	for path, h := range g.shistograms {
		n, err = fmt.Fprintf(w, "%s.mean %f %d\n", path, h.ApproximateMovingAverage(), now)
		if err != nil {
			return count, err
		}
		count += int64(n)
	}
	for path, h := range g.histograms {
		n, err = fmt.Fprintf(w, "%s.p50 %f %d\n", path, h.Quantile(0.50), now)
		n, err = fmt.Fprintf(w, "%s.p90 %f %d\n", path, h.Quantile(0.90), now)
		n, err = fmt.Fprintf(w, "%s.p95 %f %d\n", path, h.Quantile(0.95), now)
		n, err = fmt.Fprintf(w, "%s.p99 %f %d\n", path, h.Quantile(0.99), now)
		if err != nil {
			return count, err
		}
		count += int64(n)
	}
	return count, nil
}
