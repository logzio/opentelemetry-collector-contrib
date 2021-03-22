// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datadogexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/metadata"
)

const (
	// typeStr is the type of the exporter
	typeStr = "datadog"
)

// NewFactory creates a Datadog exporter factory
func NewFactory() component.ExporterFactory {
	return exporterhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		exporterhelper.WithMetrics(createMetricsExporter),
		exporterhelper.WithTraces(createTraceExporter),
	)
}

// createDefaultConfig creates the default exporter configuration
func createDefaultConfig() configmodels.Exporter {
	return &config.Config{
		ExporterSettings: configmodels.ExporterSettings{
			TypeVal: configmodels.Type(typeStr),
			NameVal: typeStr,
		},

		API: config.APIConfig{
			Key:  "$DD_API_KEY", // Must be set if using API
			Site: "$DD_SITE",    // If not provided, set during config sanitization
		},

		TagsConfig: config.TagsConfig{
			Hostname:   "$DD_HOST",
			Env:        "$DD_ENV",
			Service:    "$DD_SERVICE",
			Version:    "$DD_VERSION",
			EnvVarTags: "$DD_TAGS", // Only taken into account if Tags is not set
		},

		Metrics: config.MetricsConfig{
			TCPAddr: confignet.TCPAddr{
				Endpoint: "$DD_URL", // If not provided, set during config sanitization
			},
			SendMonotonic: true,
			DeltaTTL:      3600,
			ExporterConfig: config.MetricsExporterConfig{
				ResourceAttributesAsTags: false,
			},
		},

		Traces: config.TracesConfig{
			SampleRate: 1,
			TCPAddr: confignet.TCPAddr{
				Endpoint: "$DD_APM_URL", // If not provided, set during config sanitization
			},
		},

		SendMetadata: true,
	}
}

// createMetricsExporter creates a metrics exporter based on this config.
func createMetricsExporter(
	ctx context.Context,
	params component.ExporterCreateParams,
	c configmodels.Exporter,
) (component.MetricsExporter, error) {

	cfg := c.(*config.Config)

	params.Logger.Info("sanitizing Datadog metrics exporter configuration")
	if err := cfg.Sanitize(); err != nil {
		return nil, err
	}

	var pushMetricsFn exporterhelper.PushMetrics

	if cfg.OnlyMetadata {
		pushMetricsFn = func(context.Context, pdata.Metrics) (int, error) {
			// if only sending metadata ignore all metrics
			return 0, nil
		}
	} else {
		pushMetricsFn = newMetricsExporter(params, cfg).PushMetricsData
	}

	ctx, cancel := context.WithCancel(ctx)
	if cfg.SendMetadata {
		once := cfg.OnceMetadata()
		once.Do(func() {
			go metadata.Pusher(ctx, params, cfg)
		})
	}

	return exporterhelper.NewMetricsExporter(
		cfg,
		params.Logger,
		pushMetricsFn,
		exporterhelper.WithQueue(exporterhelper.DefaultQueueSettings()),
		exporterhelper.WithRetry(exporterhelper.DefaultRetrySettings()),
		exporterhelper.WithShutdown(func(context.Context) error {
			cancel()
			return nil
		}),
		exporterhelper.WithResourceToTelemetryConversion(exporterhelper.ResourceToTelemetrySettings{
			Enabled: cfg.Metrics.ExporterConfig.ResourceAttributesAsTags,
		}),
	)
}

// createTraceExporter creates a trace exporter based on this config.
func createTraceExporter(
	ctx context.Context,
	params component.ExporterCreateParams,
	c configmodels.Exporter,
) (component.TracesExporter, error) {

	cfg := c.(*config.Config)

	params.Logger.Info("sanitizing Datadog metrics exporter configuration")
	if err := cfg.Sanitize(); err != nil {
		return nil, err
	}

	var pushTracesFn exporterhelper.PushTraces

	if cfg.OnlyMetadata {
		pushTracesFn = func(context.Context, pdata.Traces) (int, error) {
			// if only sending metadata, ignore all traces
			return 0, nil
		}
	} else {
		pushTracesFn = newTraceExporter(params, cfg).pushTraceData
	}

	ctx, cancel := context.WithCancel(ctx)
	if cfg.SendMetadata {
		once := cfg.OnceMetadata()
		once.Do(func() {
			go metadata.Pusher(ctx, params, cfg)
		})
	}

	return exporterhelper.NewTraceExporter(
		cfg,
		params.Logger,
		pushTracesFn,
		exporterhelper.WithShutdown(func(context.Context) error {
			cancel()
			return nil
		}),
	)
}
