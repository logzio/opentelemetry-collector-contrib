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
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"gopkg.in/zorkian/go-datadog-api.v2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/utils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/ttlmap"
)

type metricsExporter struct {
	logger  *zap.Logger
	cfg     *config.Config
	client  *datadog.Client
	prevPts *ttlmap.TTLMap
}

func newMetricsExporter(params component.ExporterCreateParams, cfg *config.Config) *metricsExporter {
	client := utils.CreateClient(cfg.API.Key, cfg.Metrics.TCPAddr.Endpoint)
	client.ExtraHeader["User-Agent"] = utils.UserAgent(params.ApplicationStartInfo)
	client.HttpClient = utils.NewHTTPClient(10 * time.Second)

	utils.ValidateAPIKey(params.Logger, client)

	var sweepInterval int64 = 1
	if cfg.Metrics.DeltaTTL > 1 {
		sweepInterval = cfg.Metrics.DeltaTTL / 2
	}
	prevPts := ttlmap.New(sweepInterval, cfg.Metrics.DeltaTTL)
	prevPts.Start()

	return &metricsExporter{params.Logger, cfg, client, prevPts}
}

func (exp *metricsExporter) PushMetricsData(ctx context.Context, md pdata.Metrics) (int, error) {
	ms, droppedTimeSeries := mapMetrics(exp.cfg.Metrics, exp.prevPts, md)

	// Append the default 'running' metric
	pushTime := uint64(time.Now().UTC().UnixNano())
	ms = append(ms, metrics.DefaultMetrics("metrics", pushTime)...)

	metrics.ProcessMetrics(ms, exp.logger, exp.cfg)

	err := exp.client.PostMetrics(ms)
	return droppedTimeSeries, err
}
