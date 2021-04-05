package config

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadDefault(t *testing.T) {
	def := map[string]interface{}{
		"env":     "dev",
		"address": "localhost",
		"port":    "8080",
		"number":  10,
	}

	conf, err := Load(def, "")
	assert.NotNil(t, conf)
	assert.Nil(t, err)

	assert.Equal(t, def["env"], conf.GetString("env"))
	assert.Equal(t, def["number"], conf.GetInt("number"))
	assert.Equal(t, "", conf.GetString("any"))
}

func TestLoadEnvVar(t *testing.T) {
	def := map[string]interface{}{
		"env":     "dev",
		"address": "localhost",
		"port":    "8080",
		"number":  10,
	}

	os.Setenv("ENV", "development")
	os.Setenv("ADDRESS", "127.0.0.1")
	os.Setenv("NUMBER", "100")
	conf, err := Load(def, "")
	assert.NotNil(t, conf)
	assert.Nil(t, err)

	assert.Equal(t, "development", conf.GetString("env"))
	assert.Equal(t, "127.0.0.1", conf.GetString("address"))
	assert.Equal(t, 100, conf.GetInt("number"))
}

func TestConfigFile(t *testing.T) {
	confstr := `{"env" : "testing","port" : "8000","address" : "testhost","number": 99,"type" : "file"}`
	err := ioutil.WriteFile("./config.json", []byte(confstr), 0644)
	assert.Nil(t, err)

	def := map[string]interface{}{
		"env":     "dev",
		"address": "localhost",
		"port":    "8080",
		"number":  10,
	}

	conf, err := Load(def, "file://config.json")
	assert.NotNil(t, conf)
	assert.Nil(t, err)

	assert.Equal(t, "testing", conf.GetString("env"))
	assert.Equal(t, "testhost", conf.GetString("address"))
	assert.Equal(t, 99, conf.GetInt("number"))
	assert.Equal(t, "", conf.GetString("type"))

	os.Remove("./config.json")
}
