// Package generic implements generic versions of each of the metric types. They
// can be embedded by other implementations, and converted to specific formats
// as necessary.
package generic

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/go-kit/kit/metrics2"
)

// LabelValueUnknown is used as a label value when one is expected but not
// provided, typically due to user error.
const LabelValueUnknown = "unknown"

// Counter is an in-memory implementation of a Counter.
type Counter struct {
	bits uint64
	lvs  []string // immutable
}

// NewCounter returns a new, usable Counter.
func NewCounter() *Counter {
	return &Counter{}
}

// With implements Counter.
func (c *Counter) With(labelValues ...string) metrics.Counter {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, LabelValueUnknown)
	}
	return &Counter{
		bits: atomic.LoadUint64(&c.bits),
		lvs:  append(c.lvs, labelValues...),
	}
}

// Add implements Counter.
func (c *Counter) Add(delta float64) {
	for {
		var (
			old  = atomic.LoadUint64(&c.bits)
			newf = math.Float64frombits(old) + delta
			new  = math.Float64bits(newf)
		)
		if atomic.CompareAndSwapUint64(&c.bits, old, new) {
			break
		}
	}
}

// Value returns the current value of the counter.
func (c *Counter) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&c.bits))
}

// LabelValues returns the set of label values attached to the counter.
func (c *Counter) LabelValues() []string {
	return c.lvs
}

// Gauge is an in-memory implementation of a Gauge.
type Gauge struct {
	bits uint64
	lvs  []string // immutable
}

// NewGauge returns a new, usable Gauge.
func NewGauge() *Gauge {
	return &Gauge{}
}

// With implements Gauge.
func (c *Gauge) With(labelValues ...string) metrics.Gauge {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, LabelValueUnknown)
	}
	return &Gauge{
		bits: atomic.LoadUint64(&c.bits),
		lvs:  append(c.lvs, labelValues...),
	}
}

// Set implements Gauge.
func (c *Gauge) Set(value float64) {
	atomic.StoreUint64(&c.bits, math.Float64bits(value))
}

// Value returns the current value of the gauge.
func (c *Gauge) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&c.bits))
}

// LabelValues returns the set of label values attached to the gauge.
func (c *Gauge) LabelValues() []string {
	return c.lvs
}

// SimpleHistogram is an in-memory implementation of a Histogram. It only tracks
// an approximate moving average, so may not be suitable for all purposes.
type SimpleHistogram struct {
	mtx sync.RWMutex
	avg float64
	n   uint64
}

// NewSimpleHistogram returns a SimpleHistogram, ready to use.
func NewSimpleHistogram() *SimpleHistogram {
	return &SimpleHistogram{}
}

// Observe implements Histogram.
func (h *SimpleHistogram) Observe(value float64) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	h.n++
	h.avg -= h.avg / float64(h.n)
	h.avg += value / float64(h.n)
}

// ApproximateMovingAverage returns the approximate moving average of observations.
func (h *SimpleHistogram) ApproximateMovingAverage() float64 {
	h.mtx.RLock()
	h.mtx.RUnlock()
	return h.avg
}
