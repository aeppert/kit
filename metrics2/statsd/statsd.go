// Package statsd implements a statsd backend for package metrics. Metrics are
// aggregated and reported in the statsd plaintext format. Sampling is not
// supported for counters because we aggregate counter updates and send in
// batches. Sampling is, however, supported for Timings.
//
// Typically you'll create a statsd object in your main function.
//
//    s, stop := New("myorg.myteam.", "udp", "statsd:8126", time.Second, log.NewNopLogger())
//    defer stop()
//
// Then, create the metrics that your application will track from that object.
// Pass them as dependencies to the component that needs them. Don't place them
// in the global scope.
//
//    c := s.NewCounter("requests")
//    g := s.NewGauge("queue_depth")
//    t := s.NewTiming("foo_duration", "ms", 1.0)
//    h := s.MustNewHistogram("bar_duration", time.Second, time.Millisecond, 1.0)
//
// Invoke them in your components when you have something to instrument.
//
//    c.Add(1)
//    g.Set(123)
//    t.Observe(16)    // 16 ms
//    h.Observe(0.032) // 32 ms
//
package statsd

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics2"
	"github.com/go-kit/kit/metrics2/generic"
	"github.com/go-kit/kit/util/conn"
)

// Statsd is a collection of individual metrics. To send metrics to a statsd
// server, create a statsd object, use it to create metrics objects, and pass
// those objects as dependencies to the components that will use them.
//
// Statsd has a concept of Timings rather than Histograms. You can create Timing
// objects, or create Histograms that wrap Timings under the hood.
type Statsd struct {
	mtx      sync.RWMutex
	prefix   string
	counters map[string]*generic.Counter
	gauges   map[string]*generic.Gauge
	timings  map[string]*Timing
	logger   log.Logger
}

// NewRaw creates a Statsd object. By default the metrics will not be emitted
// anywhere. Use WriteTo to flush the metrics once, or FlushTo (in a separate
// goroutine) to flush them on a regular schedule, or use the New constructor to
// set up the object and flushing at the same time.
func NewRaw(prefix string, logger log.Logger) *Statsd {
	return &Statsd{
		prefix:   prefix,
		counters: map[string]*generic.Counter{},
		gauges:   map[string]*generic.Gauge{},
		timings:  map[string]*Timing{},
		logger:   logger,
	}
}

// New creates a Statsd object that flushes all metrics in the statsd format
// every flushInterval to the network and address. Use the returned stop
// function to terminate the flushing goroutine.
func New(prefix string, network, address string, flushInterval time.Duration, logger log.Logger) (res *Statsd, stop func()) {
	s := NewRaw(prefix, logger)
	manager := conn.NewDefaultManager(network, address, logger)
	ticker := time.NewTicker(flushInterval)
	go s.FlushTo(manager, ticker)
	return s, ticker.Stop
}

// NewCounter returns a counter metric with the given name. Adds are buffered
// until the underlying statsd object is flushed.
func (s *Statsd) NewCounter(name string) *generic.Counter {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	c := generic.NewCounter()
	s.counters[s.prefix+name] = c
	return c
}

// NewGauge returns a gauge metric with the given name.
func (s *Statsd) NewGauge(name string) *generic.Gauge {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	g := generic.NewGauge()
	s.gauges[s.prefix+name] = g
	return g
}

// NewTiming returns a timing metric with the given name, unit (e.g. "ms") and
// sample rate. Pass a sample rate of 1.0 or greater to disable sampling.
// Sampling is done at observation time. Observations are buffered until the
// underlying statsd object is flushed.
func (s *Statsd) NewTiming(name, unit string, sampleRate float64) *Timing {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	t := NewTiming(unit, sampleRate)
	s.timings[s.prefix+name] = t
	return t
}

// NewHistogram returns a histogram metric with the given name. Observations are
// assumed to be taken in units of observeIn, e.g. time.Second. The histogram
// wraps a timing which reports in units of reportIn, e.g. time.Millisecond.
// Only nanoseconds, microseconds, milliseconds, and seconds are supported
// reportIn values. The underlying timing is sampled according to sampleRate.
func (s *Statsd) NewHistogram(name string, observeIn, reportIn time.Duration, sampleRate float64) (metrics.Histogram, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var unit string
	switch reportIn {
	case time.Nanosecond:
		unit = "ns"
	case time.Microsecond:
		unit = "us"
	case time.Millisecond:
		unit = "ms"
	case time.Second:
		unit = "s"
	default:
		return nil, errors.New("unsupported reporting duration")
	}

	t := NewTiming(unit, sampleRate)
	s.timings[s.prefix+name] = t
	return newHistogram(observeIn, reportIn, t), nil
}

// MustNewHistogram is a convenience constructor for NewHistogram, which panics
// if there is an error.
func (s *Statsd) MustNewHistogram(name string, observeIn, reportIn time.Duration, sampleRate float64) metrics.Histogram {
	h, err := s.NewHistogram(name, observeIn, reportIn, sampleRate)
	if err != nil {
		panic(err)
	}
	return h
}

// FlushTo invokes WriteTo to the writer every time the ticker fires.
// FlushTo blocks until the ticker is stopped.
// See the example for typical usage.
func (s *Statsd) FlushTo(w io.Writer, ticker *time.Ticker) {
	for range ticker.C {
		if _, err := s.WriteTo(w); err != nil {
			s.logger.Log("during", "Flush", "err", err)
		}
	}
}

// WriteTo dumps the current state of all of the Statsd metrics to the given
// writer in the statsd format. Counters and gauges are dumped with their
// current values; counters are reset. Timings write each retained observation
// in sequence, and are reset.
func (s *Statsd) WriteTo(w io.Writer) (int64, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	var (
		n     int
		err   error
		count int64
	)
	for name, c := range s.counters {
		n, err = fmt.Fprintf(w, "%s:%f|c\n", name, c.ValueReset())
		count += int64(n)
		if err != nil {
			return count, err
		}
	}
	for name, g := range s.gauges {
		n, err = fmt.Fprintf(w, "%s:%f|g\n", name, g.Value())
		count += int64(n)
		if err != nil {
			return count, err
		}
	}
	for name, t := range s.timings {
		var sampling string
		if r := t.sampleRate; r < 1.0 {
			sampling = fmt.Sprintf("|@%f", r)
		}
		for _, value := range t.Values() {
			n, err = fmt.Fprintf(w, "%s:%d|%s%s\n", name, value, t.unit, sampling)
			count += int64(n)
			if err != nil {
				return count, err
			}
		}
	}
	return count, nil
}

// Timing is used like a histogram, but has a different implementation. Statsd
// expects you to emit each observation to the aggregation server, and they do
// statistical processing there. This is easier to understand, but much (much)
// less efficient. So, we batch observations and emit the batch every interval.
// And, we support sampling
type Timing struct {
	mtx        sync.Mutex
	unit       string
	sampleRate float64
	values     []int64
}

// NewTiming returns a new Timing object with the given units (e.g. "ms") and
// sample rate. If sample rate >= 1.0, no sampling will be performed.
func NewTiming(unit string, sampleRate float64) *Timing {
	return &Timing{
		unit:       unit,
		sampleRate: sampleRate,
	}
}

// Observe collects the value into the timing. If sample rate is less than 1.0,
// sampling is performed, and the value may be dropped.
func (t *Timing) Observe(value int64) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	// Here we sample at observation time. This burns not-insignificant CPU in
	// the rand.Float64 call. It may be preferable to aggregate all observations
	// and sample at emission time. But that is a bit tricker to do correctly.

	if t.sampleRate < 1.0 && rand.Float64() > t.sampleRate {
		return
	}
	t.values = append(t.values, value)
}

// Values returns the observed values since the last call to values. This method
// clears the internal state of the Timing; better get those values somewhere
// safe!
func (t *Timing) Values() []int64 {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	res := t.values
	t.values = []int64{} // TODO(pb): better garbage behavior possible?
	return res
}

// histogram wraps a Timing and implements Histogram. Namely, it takes float64
// observations and converts them to int64 according to a defined ratio, likely
// with a loss of precision.
type histogram struct {
	m   float64
	t   *Timing
	lvs []string
}

func newHistogram(observeIn, reportIn time.Duration, t *Timing) *histogram {
	return &histogram{
		m: float64(observeIn) / float64(reportIn),
		t: t,
	}
}

func (h *histogram) With(labelValues ...string) metrics.Histogram {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, generic.LabelValueUnknown)
	}
	return &histogram{
		m:   h.m,
		t:   h.t,
		lvs: append(h.lvs, labelValues...),
	}
}

func (h *histogram) Observe(value float64) {
	h.t.Observe(int64(h.m * value))
}
