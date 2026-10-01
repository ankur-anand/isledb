package isledb

import (
	"reflect"
	"testing"
)

// TestNilMetricsAreSafe calls every exported method of nil writer and reader
// metrics with zero arguments: metrics are optional, so each must check its
// receiver before touching a field.
func TestNilMetricsAreSafe(t *testing.T) {
	for _, metrics := range []any{(*WriterMetrics)(nil), (*ReaderMetrics)(nil)} {
		value := reflect.ValueOf(metrics)
		for i := range value.NumMethod() {
			method := value.Type().Method(i)
			args := make([]reflect.Value, method.Type.NumIn()-1)
			for j := range args {
				args[j] = reflect.Zero(method.Type.In(j + 1))
			}
			t.Run(value.Type().Elem().Name()+"."+method.Name, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("panicked on nil metrics: %v", r)
					}
				}()
				value.Method(i).Call(args)
			})
		}
	}
}
