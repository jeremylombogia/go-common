package logger

import (
	"context"
	"strings"

	"github.com/sahalazain/go-common/config"
	"github.com/sirupsen/logrus"
)

var log *logrus.Entry

type logConfig struct {
	Name   string `json:"name,omitempty" mapstructure:"name"`
	Format string `json:"format,omitempty" mapstructure:"format"`
	Level  string `json:"level,omitempty" mapstructure:"level"`
}

func (l *logConfig) init() {
	if l.Format == "" {
		l.Format = "txt"
	}

	if l.Level == "" {
		l.Level = "debug"
	}

	if l.Name == "" {
		l.Name = "default"
	}

	switch l.Format {
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{})
	default:
		logrus.SetFormatter(&logrus.TextFormatter{})
	}

	switch strings.ToLower(l.Level) {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}

	log = logrus.WithField("app", l.Name)
}

//Configure configure log
func Configure(conf config.Getter) {
	var lconf logConfig
	conf.Unmarshal(&lconf)
	lconf.init()
}

func setDefault() {
	lconf := &logConfig{}
	lconf.init()
}

//GetLogger get logger instance
func GetLogger(pkg, fnName string) *logrus.Entry {
	if log == nil {
		setDefault()
	}
	return log.WithFields(logrus.Fields{
		"function": fnName,
		"package":  pkg,
	})
}

//GetLoggerContext get logger with context
func GetLoggerContext(ctx context.Context, pkg, fnName string) *logrus.Entry {
	if log == nil {
		setDefault()
	}
	return log.WithContext(ctx).WithFields(logrus.Fields{
		"function": fnName,
		"package":  pkg,
	})
}
