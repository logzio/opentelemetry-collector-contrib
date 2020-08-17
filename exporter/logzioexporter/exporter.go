package logzioexporter

import (
	"fmt"
	"github.com/logzio/logzio-go"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
)

const (
	dropLogsDiskThreshold = 98
	usRegionCode 		  = "us"
	logzioListenerUrl	  = "https://listener%s.logz.io:8071"
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

func newLogzioExporter(logger *zap.Logger, exporter configmodels.Exporter) (*logzioExporter, error) {
	logzioConfig, ok := exporter.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config: %#v", exporter)
	}

	sender, err := logzio.New(
		logzioConfig.Token,
		logzio.SetUrl(fmt.Sprintf(logzioListenerUrl, regionCode(logzioConfig.Region))),
		logzio.SetDrainDiskThreshold(dropLogsDiskThreshold),
		logzio.SetDebug(logWriter{logf: logger.Debug}),
	)
	if err != nil {
		return nil, err
	}

	return &logzioExporter{
		sender:			sender,
		accountToken:	logzioConfig.Token,
	}, nil
}

func regionCode(region string) string {
	regionCode := ""
	if region != "" && region != usRegionCode {
		regionCode = fmt.Sprintf("-%s", region)
	}
	return regionCode
}
