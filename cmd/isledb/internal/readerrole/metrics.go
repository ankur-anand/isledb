package readerrole

import (
	"reflect"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/internal/readhttp"
	"github.com/prometheus/client_golang/prometheus"
)

// readerCollectors returns the library's reader metrics as collectors: every
// field of metrics that is one.
func readerCollectors(metrics *isledb.ReaderMetrics) []prometheus.Collector {
	var out []prometheus.Collector
	v := reflect.ValueOf(metrics).Elem()
	for i := range v.NumField() {
		if c, ok := v.Field(i).Interface().(prometheus.Collector); ok && !v.Field(i).IsNil() {
			out = append(out, c)
		}
	}
	return out
}

// collector reports the reader's state as it is at each scrape.
type collector struct {
	reader  *isledb.Reader
	handler *readhttp.Handler
}

var (
	descPosition   = prometheus.NewDesc("isledb_reader_view_position", "Position of the view reads are answered from.", nil, nil)
	descInProgress = prometheus.NewDesc("isledb_reader_http_reads_in_progress", "Reads being served now.", nil, nil)
)

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descPosition
	ch <- descInProgress
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(descPosition, prometheus.GaugeValue, float64(c.reader.ViewPosition()))
	ch <- prometheus.MustNewConstMetric(descInProgress, prometheus.GaugeValue, float64(c.handler.InProgress()))
}
