package event

import (
	"context"
	"testing"

	"github.com/sahalazain/go-common/config"
	_ "github.com/sahalazain/simplecache/mem"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/docstore/memdocstore"
)

func TestOutbox(t *testing.T) {
	cfg := map[string]interface{}{
		"collection_url": "mem://outbox/_id",
		"cache_url":      "mem://oc",
	}
	conf, err := config.Load(cfg, "")
	assert.Nil(t, err)
	assert.NotNil(t, conf)

	ctx := context.Background()

	out, err := NewOutboxEmitter(ctx, conf)
	assert.Nil(t, err)
	assert.NotNil(t, out)

	obj := map[string]interface{}{
		"name":    "SiCepat",
		"address": "Jakarta",
	}

	err = out.Publish(ctx, "test", obj, nil)
	assert.Nil(t, err)

	err = out.Publish(ctx, "test", obj, nil)
	assert.Nil(t, err)
}
