package service

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// httpMetrics counts a role's API requests and their latency by route.
type httpMetrics struct {
	route    func(*http.Request) string
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

func newHTTPMetrics(reg prometheus.Registerer, role string, route func(*http.Request) string) *httpMetrics {
	if route == nil {
		route = func(*http.Request) string { return "api" }
	}
	m := &httpMetrics{
		route: route,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "isledb_" + role + "_http_requests_total", Help: "API requests, by route and status code.",
		}, []string{"route", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "isledb_" + role + "_http_request_seconds", Help: "Time to answer an API request, by route.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 18),
		}, []string{"route"}),
	}
	reg.MustRegister(m.requests, m.latency)
	return m
}

func (m *httpMetrics) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := m.route(r)
		m.requests.WithLabelValues(route, strconv.Itoa(rec.code)).Inc()
		m.latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the connection, to set write
// deadlines.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
