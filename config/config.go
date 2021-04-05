package config

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const (
	defaultType  = "json"
	configURLEnv = "CONFIG_URL"
)

//Load load configuration from config url, by default will load environment variable
func Load(def map[string]interface{}, urlStr string) (Getter, error) {

	// first lets load .env file
	econf := viper.New()
	config := viper.New()

	for k, v := range def {
		econf.BindEnv(k)
		config.SetDefault(k, v)
		if econf.IsSet(k) {
			config.Set(k, econf.Get(k))
		}
	}

	if urlStr == "" {
		urlStr = os.Getenv(configURLEnv)
	}

	if urlStr == "" {
		return config, nil
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	aconf := viper.New()

	switch u.Scheme {
	case "file":
		path := strings.TrimPrefix(urlStr, "file://")
		name := filepath.Base(path)
		t := strings.TrimPrefix(filepath.Ext(path), ".")
		path = filepath.Dir(path)
		aconf.SetConfigName(name)
		aconf.SetConfigType(t)
		aconf.AddConfigPath(path)
		if err := aconf.ReadInConfig(); err != nil {
			return nil, err
		}
	case "consul":
		host := u.Host
		key := strings.TrimPrefix(u.Path, "/")
		aconf.AddRemoteProvider("consul", host, key)
		aconf.SetConfigType(defaultType) // Need to explicitly set this to json
		if err := aconf.ReadRemoteConfig(); err != nil {
			return nil, err
		}
	case "etcd":
		host := u.Host
		path := u.Path
		aconf.AddRemoteProvider("etcd", host, path)
		aconf.SetConfigType(defaultType)
		if err := aconf.ReadRemoteConfig(); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("Unsupported config scheme")
	}

	for k := range def {
		if aconf.IsSet(k) {
			config.Set(k, aconf.Get(k))
		}
	}

	return config, nil
}

//Getter config getter interface
type Getter interface {
	Get(k string) interface{}
	GetString(k string) string
	GetBool(k string) bool
	GetInt(k string) int
	GetFloat64(k string) float64
	GetStringSlice(k string) []string
	GetStringMap(k string) map[string]interface{}
	GetStringMapString(k string) map[string]string
	Unmarshal(rawVal interface{}, opts ...viper.DecoderConfigOption) error
}
