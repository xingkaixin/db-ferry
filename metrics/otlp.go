package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

func (r *PrometheusRecorder) pushOTLP(ctx context.Context) error {
	payload, err := r.buildOTLPJSON()
	if err != nil {
		return fmt.Errorf("failed to build OTLP payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to push metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("otlp push returned status %d", resp.StatusCode)
	}
	return nil
}

func (r *PrometheusRecorder) buildOTLPJSON() ([]byte, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	rm := otlpResourceMetrics{
		Resource: otlpResource{
			Attributes: []otlpAttribute{
				{Key: "service.name", Value: otlpAnyValue{StringValue: "db-ferry"}},
				{Key: "service.version", Value: otlpAnyValue{StringValue: r.version}},
			},
		},
		ScopeMetrics: []otlpScopeMetrics{
			{
				Scope:   otlpScope{Name: "db-ferry", Version: r.version},
				Metrics: r.buildOTLPMetrics(now),
			},
		},
	}

	return json.Marshal(map[string]any{"resourceMetrics": []otlpResourceMetrics{rm}})
}

func (r *PrometheusRecorder) buildOTLPMetrics(now string) []otlpMetric {
	var metrics []otlpMetric

	r.rowsProcessed.Range(func(key, value any) bool {
		parts := strings.Split(key.(string), "\x00")
		v := value.(*atomic.Int64).Load()
		metrics = append(metrics, r.newSumMetric("db_ferry_task_rows_processed", now, float64(v), parts...))
		return true
	})

	r.batchesTotal.Range(func(key, value any) bool {
		parts := strings.Split(key.(string), "\x00")
		v := value.(*atomic.Int64).Load()
		status := ""
		if len(parts) >= 4 {
			status = parts[3]
		}
		attrs := r.baseAttrs(parts...)
		if status != "" {
			attrs = append(attrs, otlpAttribute{Key: "status", Value: otlpAnyValue{StringValue: status}})
		}
		metrics = append(metrics, otlpMetric{
			Name: "db_ferry_task_batches_total",
			Sum: &otlpSum{
				DataPoints: []otlpNumberDataPoint{{
					Attributes:   attrs,
					AsDouble:     float64(v),
					TimeUnixNano: now,
				}},
				AggregationTemporality: 2,
				IsMonotonic:            true,
			},
		})
		return true
	})

	r.dlqRows.Range(func(key, value any) bool {
		parts := strings.Split(key.(string), "\x00")
		v := value.(*atomic.Int64).Load()
		metrics = append(metrics, r.newSumMetric("db_ferry_task_dlq_rows_total", now, float64(v), parts...))
		return true
	})

	r.validationMismatches.Range(func(key, value any) bool {
		parts := strings.Split(key.(string), "\x00")
		v := value.(*atomic.Int64).Load()
		validateType := ""
		if len(parts) >= 4 {
			validateType = parts[3]
		}
		attrs := r.baseAttrs(parts...)
		if validateType != "" {
			attrs = append(attrs, otlpAttribute{Key: "validate_type", Value: otlpAnyValue{StringValue: validateType}})
		}
		metrics = append(metrics, otlpMetric{
			Name: "db_ferry_task_validation_mismatches_total",
			Sum: &otlpSum{
				DataPoints: []otlpNumberDataPoint{{
					Attributes:   attrs,
					AsDouble:     float64(v),
					TimeUnixNano: now,
				}},
				AggregationTemporality: 2,
				IsMonotonic:            true,
			},
		})
		return true
	})

	r.batchDurationMu.RLock()
	for k, e := range r.batchDuration {
		parts := strings.Split(k, "\x00")
		attrs := r.baseAttrs(parts...)
		metrics = append(metrics, otlpMetric{
			Name: "db_ferry_task_batch_duration_ms_sum",
			Sum: &otlpSum{
				DataPoints: []otlpNumberDataPoint{{
					Attributes:   attrs,
					AsDouble:     e.loadSum(),
					TimeUnixNano: now,
				}},
				AggregationTemporality: 2,
				IsMonotonic:            true,
			},
		})
		metrics = append(metrics, otlpMetric{
			Name: "db_ferry_task_batch_duration_ms_count",
			Sum: &otlpSum{
				DataPoints: []otlpNumberDataPoint{{
					Attributes:   attrs,
					AsDouble:     float64(e.count.Load()),
					TimeUnixNano: now,
				}},
				AggregationTemporality: 2,
				IsMonotonic:            true,
			},
		})
	}
	r.batchDurationMu.RUnlock()

	r.taskDurationMu.RLock()
	for k, e := range r.taskDuration {
		parts := strings.Split(k, "\x00")
		attrs := r.baseAttrs(parts...)
		metrics = append(metrics, otlpMetric{
			Name: "db_ferry_task_duration_ms_sum",
			Sum: &otlpSum{
				DataPoints: []otlpNumberDataPoint{{
					Attributes:   attrs,
					AsDouble:     e.loadSum(),
					TimeUnixNano: now,
				}},
				AggregationTemporality: 2,
				IsMonotonic:            true,
			},
		})
		metrics = append(metrics, otlpMetric{
			Name: "db_ferry_task_duration_ms_count",
			Sum: &otlpSum{
				DataPoints: []otlpNumberDataPoint{{
					Attributes:   attrs,
					AsDouble:     float64(e.count.Load()),
					TimeUnixNano: now,
				}},
				AggregationTemporality: 2,
				IsMonotonic:            true,
			},
		})
	}
	r.taskDurationMu.RUnlock()

	return metrics
}

func (r *PrometheusRecorder) baseAttrs(parts ...string) []otlpAttribute {
	taskName := ""
	sourceDB := ""
	targetDB := ""
	if len(parts) >= 1 {
		taskName = parts[0]
	}
	if len(parts) >= 2 {
		sourceDB = parts[1]
	}
	if len(parts) >= 3 {
		targetDB = parts[2]
	}
	return []otlpAttribute{
		{Key: "task_name", Value: otlpAnyValue{StringValue: taskName}},
		{Key: "source_db", Value: otlpAnyValue{StringValue: sourceDB}},
		{Key: "target_db", Value: otlpAnyValue{StringValue: targetDB}},
		{Key: "version", Value: otlpAnyValue{StringValue: r.version}},
	}
}

func (r *PrometheusRecorder) newSumMetric(name, now string, value float64, parts ...string) otlpMetric {
	return otlpMetric{
		Name: name,
		Sum: &otlpSum{
			DataPoints: []otlpNumberDataPoint{{
				Attributes:   r.baseAttrs(parts...),
				AsDouble:     value,
				TimeUnixNano: now,
			}},
			AggregationTemporality: 2,
			IsMonotonic:            true,
		},
	}
}

type otlpResourceMetrics struct {
	Resource     otlpResource       `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpResource struct {
	Attributes []otlpAttribute `json:"attributes"`
}

type otlpScopeMetrics struct {
	Scope   otlpScope    `json:"scope"`
	Metrics []otlpMetric `json:"metrics"`
}

type otlpScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type otlpMetric struct {
	Name  string     `json:"name"`
	Sum   *otlpSum   `json:"sum,omitempty"`
	Gauge *otlpGauge `json:"gauge,omitempty"`
}

type otlpSum struct {
	DataPoints             []otlpNumberDataPoint `json:"dataPoints"`
	AggregationTemporality int                   `json:"aggregationTemporality"`
	IsMonotonic            bool                  `json:"isMonotonic"`
}

type otlpGauge struct {
	DataPoints []otlpNumberDataPoint `json:"dataPoints"`
}

type otlpNumberDataPoint struct {
	Attributes   []otlpAttribute `json:"attributes"`
	AsDouble     float64         `json:"asDouble"`
	TimeUnixNano string          `json:"timeUnixNano"`
}

type otlpAttribute struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}
