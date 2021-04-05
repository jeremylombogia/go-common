package logger

import (
	"testing"

	"github.com/sahalazain/go-common/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestDefaultLogger(t *testing.T) {
	assert.Nil(t, log)
	setDefault()
	assert.NotNil(t, log)
	assert.Equal(t, logrus.DebugLevel, log.Logger.Level)
}

func TestLoadConfig(t *testing.T) {
	conf := map[string]interface{}{
		"level":  "error",
		"format": "json",
		"name":   "test",
	}
	assert.Nil(t, log)
	cfg, err := config.Load(conf, "")
	assert.Nil(t, err)
	assert.NotNil(t, cfg)

	Configure(cfg)
	assert.NotNil(t, log)
	assert.Equal(t, logrus.ErrorLevel, log.Logger.Level)
}
