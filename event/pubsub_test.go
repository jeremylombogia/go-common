package event

import (
	"context"
	"testing"

	"github.com/sahalazain/go-common/config"
	_ "github.com/sahalazain/simplecache/mem"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/pubsub/mempubsub"
)

func TestPubsub(t *testing.T) {
	cfg := map[string]interface{}{
		"cache_url":  "mem://pc",
		"pubsub_url": "mem://$TOPIC",
	}
	conf, err := config.Load(cfg, "")
	assert.Nil(t, err)
	assert.NotNil(t, conf)

	ctx := context.Background()

	ps, err := NewPubSubEmitter(ctx, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ps)

	obj := map[string]interface{}{
		"name":    "SiCepat",
		"address": "Jakarta",
	}

	err = ps.Publish(ctx, "test", obj, nil)
	assert.Nil(t, err)

	err = ps.Publish(ctx, "test", obj, nil)
	assert.Nil(t, err)
}
