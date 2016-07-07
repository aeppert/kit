package prometheus

import (
	"github.com/go-kit/kit/metrics2"
	"github.com/prometheus/client_golang/prometheus"
)

var LabelValueUnknown = "unknown"

type Counter struct {
	cv *prometheus.CounterVec
	lv []string
}

func NewCounterFrom(opts prometheus.CounterOpts, labelNames []string) *Counter {
	cv := prometheus.NewCounterVec(opts, labelNames)
	prometheus.MustRegister(cv)
	return NewCounter(cv)
}

func NewCounter(cv *prometheus.CounterVec) *Counter {
	return &Counter{
		cv: cv,
		lv: []string{},
	}
}

func (c *Counter) With(labelValues ...string) metrics.Counter {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, LabelValueUnknown)
	}
	return &Counter{
		cv: c.cv,
		lv: append(c.lv, labelValues...),
	}
}

func (c *Counter) Add(delta float64) {
	c.cv.WithLabelValues(c.lv...).Add(delta)
}

type Gauge struct {
	gv *prometheus.GaugeVec
	lv []string
}

func NewGaugeFrom(opts prometheus.GaugeOpts, labelNames []string) *Gauge {
	gv := prometheus.NewGaugeVec(opts, labelNames)
	prometheus.MustRegister(gv)
	return NewGauge(gv)
}

func NewGauge(gv *prometheus.GaugeVec) *Gauge {
	return &Gauge{
		gv: gv,
		lv: []string{},
	}
}

func (g *Gauge) With(labelValues ...string) metrics.Gauge {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, LabelValueUnknown)
	}
	return &Gauge{
		gv: g.gv,
		lv: append(g.lv, labelValues...),
	}
}

func (g *Gauge) Set(value float64) {
	g.gv.WithLabelValues(g.lv...).Set(value)
}

type Summary struct {
	sv *prometheus.SummaryVec
	lv []string
}

func NewSummaryFrom(opts prometheus.SummaryOpts, labelNames []string) *Summary {
	sv := prometheus.NewSummaryVec(opts, labelNames)
	prometheus.MustRegister(sv)
	return NewSummary(sv)
}

func NewSummary(sv *prometheus.SummaryVec) *Summary {
	return &Summary{
		sv: sv,
		lv: []string{},
	}
}

func (s *Summary) With(labelValues ...string) metrics.Histogram {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, LabelValueUnknown)
	}
	return &Summary{
		sv: s.sv,
		lv: append(s.lv, labelValues...),
	}
}

func (s *Summary) Observe(value float64) {
	s.sv.WithLabelValues(s.lv...).Observe(value)
}

type Histogram struct {
	hv *prometheus.HistogramVec
	lv []string
}

func NewHistogramFrom(opts prometheus.HistogramOpts, labelNames []string) *Summary {
	hv := prometheus.NewHistogramVec(opts, labelNames)
	prometheus.MustRegister(hv)
	return NewHistogram(hv)
}

func NewHistogram(hv *prometheus.HistogramVec) *Summary {
	return &Summary{
		hv: hv,
		lv: []string{},
	}
}

func (h *Histogram) With(labelValues ...string) metrics.Histogram {
	if len(labelValues)%2 != 0 {
		labelValues = append(labelValues, LabelValueUnknown)
	}
	return &Histogram{
		hv: h.hv,
		lv: append(h.lv, labelValues...),
	}
}

func (h *Histogram) Observe(value float64) {
	h.hv.WithLabelValues(h.lv...).Observe(value)
}
