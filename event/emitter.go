package event

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/sahalazain/go-common/config"
	"github.com/sahalazain/simplecache"
)

//Emitter event emitter
type Emitter interface {
	Publish(ctx context.Context, event string, message interface{}, metadata map[string]interface{}) error
	Push(ctx context.Context, event, key string, message interface{}, metadata map[string]interface{}) error
}

// EventMessage event message
type EventMessage struct {
	Data     interface{}            `json:"data,omitempty" mapstructure:"data"`
	Metadata map[string]interface{} `json:"metadata,omitempty" mapstructure:"metadata"`
}

func (m *EventMessage) ToBytes() ([]byte, error) {
	return json.Marshal(m)
}

//NewEmitter create event emitter instance
func NewEmitter(ctx context.Context, conf config.Getter) (Emitter, error) {
	if conf == nil {
		return nil, errors.New("[Emitter] missing event_emitter param")
	}

	switch strings.ToLower(conf.GetString("type")) {
	case "pubsub":
		return NewPubSubEmitter(ctx, conf)
	case "outbox":
		return NewOutboxEmitter(ctx, conf)
	case "hybrid":
		return NewHybridEmitter(ctx, conf)
	default:
		return nil, errors.New("[Emitter] unsupported emitter")
	}
}

// NewEmitterCache new emitter cache
func NewEmitterCache(URL string) (*EmitterCache, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	cache, err := simplecache.New(URL)
	if err != nil {
		return nil, err
	}

	return &EmitterCache{
		cache:   cache,
		keyName: strings.Trim(u.Path, "/") + "/",
	}, nil
}

// EmitterCache emitter key cache
type EmitterCache struct {
	cache   simplecache.Cache
	keyName string
}

func (e *EmitterCache) getPrevious(ctx context.Context, event string) string {
	if e.cache == nil {
		return ""
	}
	k, _ := e.cache.GetString(ctx, e.keyName+event)
	return k
}

func (e *EmitterCache) setCurrent(ctx context.Context, event, val string) error {
	if e.cache == nil {
		return errors.New("[Emitter] empty cache")
	}
	return e.cache.Set(ctx, e.keyName+event, val, 0)
}

type EventConfig struct {
	Metadata map[string]map[string]interface{} `json:"metadata,omitempty" mapstructure:"metadata"`
	EventMap map[string]string                 `json:"event_map,omitempty" mapstructure:"event_map"`
}

func (c *EventConfig) getTopic(event string) string {
	if t, ok := c.EventMap[event]; ok {
		return t
	}
	return event
}

func (c *EventConfig) getMetadata(event string) map[string]interface{} {
	if m, ok := c.Metadata[event]; ok {
		m["event"] = event
		return m
	}
	return c.getDefaultMetadata(event)
}

func (c *EventConfig) getDefaultMetadata(event string) map[string]interface{} {
	if m, ok := c.Metadata["default"]; ok {
		m["event"] = event
		return m
	}

	return map[string]interface{}{
		"version": 1,
		"event":   event,
	}
}

func hash(m interface{}) (string, error) {
	mb, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	k := sha256.Sum256(mb)

	return string(base64.StdEncoding.EncodeToString(k[:])), nil
}
