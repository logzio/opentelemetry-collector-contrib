package logzioexporter

import (
	"context"
	"fmt"
	"github.com/logzio/logzio-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/consumer/consumerdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
)

const (
	dropLogsDiskThreshold = 98
	usRegionCode 		  = "us"
	logzioListenerUrl	  = "https://liste	ner%s.logz.io:8071"
)

var _ io.Writer = logWriter{}

// logWriter wraps a zap.Logger into an io.Writer.
type logWriter struct {
	logf func(string, ...zapcore.Field)
}

// Write implements io.Writer
func (w logWriter) Write(p []byte) (n int, err error) {
	w.logf(string(p))
	return len(p), nil
}

// exporter exporters OpenTelemetry Collector data to New Relic.
type logzioExporter struct {
	accountToken string
	sender       *logzio.LogzioSender
}

func newLogzioExporter(config *Config, params component.ExporterCreateParams) (*logzioExporter, error) {
	sender, err := logzio.New(
		config.Token,
		logzio.SetUrl(fmt.Sprintf(logzioListenerUrl, regionCode(config.Region))),
		logzio.SetDrainDiskThreshold(dropLogsDiskThreshold),
		//logzio.SetDebug(logWriter{logf: logger.Debug}),
	)
	if err != nil {
		return nil, err
	}

	return &logzioExporter{
		sender:			sender,
		accountToken:	config.Token,
	}, nil
}

func newLogzioTraceExporter(config *Config, params component.ExporterCreateParams) (component.TraceExporter, error) {
	exporter, err := newLogzioExporter(config, params)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewTraceExporter(
		config,
		exporter.pushTraceData,
		exporterhelper.WithShutdown(exporter.Shutdown))
}

func (se *sapmExporter) pushTraceData(ctx context.Context, td pdata.Traces) (droppedSpansCount int, err error) {
	var errs []error
	goodSpans := 0

	for _, span := range td.Spans {
		if span.IsNil() {
			// Invalid trace so nothing to export
			continue
		}
		nrSpan, err := transform.Span(span)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		err = exporter.harvester.RecordSpan(nrSpan)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		goodSpans++
	}

	exporter.harvester.HarvestNow(ctx)

	return len(td.Spans) - goodSpans, componenterror.CombineErrors(errs)
}

func (exporter *logzioExporter) Shutdown(ctx context.Context) error {
	exporter.sender.Stop()
	return nil
}

func regionCode(region string) string {
	regionCode := ""
	if region != "" && region != usRegionCode {
		regionCode = fmt.Sprintf("-%s", region)
	}
	return regionCode
}
