package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sahalazain/go-common/config"
	"github.com/sahalazain/go-common/util"
	_ "github.com/sahalazain/simplecache/mem"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/docstore/memdocstore"
)

type Person struct {
	ID        string    `json:"_id,omitempty" mapstructure:"_id" docstore:"_id" bson:"_id"`
	Name      string    `json:"name,omitempty" mapstructure:"name" docstore:"name"`
	Age       int       `json:"age,omitempty" mapstructure:"age" docstore:"age"`
	CreatedAt time.Time `json:"created_at,omitempty" mapstructure:"created_at" docstore:"created_at" bson:"created_at"`
}

// GenerateID generate record ID
func (p *Person) GenerateID() Document {
	if p.ID != "" {
		return p
	}
	p.ID = util.Hash58(p)
	return p
}

// GetID get model id
func (p *Person) GetID() string {
	return p.ID
}

// SetCreatedTime set created time
func (p *Person) SetCreatedTime(ti time.Time) Document {
	p.CreatedAt = ti
	return p
}

// GetCreatedTime get created time
func (p *Person) GetCreatedTime() time.Time {
	return p.CreatedAt
}

func TestModelCreate(t *testing.T) {
	cfg := map[string]interface{}{
		"collection_url": "mem://tmp/_id",
		"cache_url":      "mem://tcache",
	}
	conf, err := config.Load(cfg, "")
	assert.Nil(t, err)
	assert.NotNil(t, conf)

	ctx := context.Background()
	rep, err := New(ctx, conf)
	assert.Nil(t, err)
	assert.NotNil(t, rep)
	assert.Equal(t, defaultExpiration, rep.CacheExpiration)

	tmp := &Person{
		Name: "test",
		Age:  25,
	}

	err = rep.Create(ctx, tmp)
	assert.Nil(t, err)

	assert.NotEqual(t, "", tmp.ID)

	out := Person{
		ID: tmp.ID,
	}
	err = rep.Get(ctx, &out)
	assert.Nil(t, err)
	assert.Equal(t, tmp.ID, out.ID)
	assert.Equal(t, tmp.Name, out.Name)

	out2 := Person{
		ID: tmp.ID,
	}
	err = rep.Get(ctx, &out2)
	assert.Nil(t, err)
	assert.Equal(t, tmp.ID, out2.ID)
	assert.Equal(t, tmp.Name, out2.Name)

}

func TestModelFind(t *testing.T) {
	cfg := map[string]interface{}{
		"collection_url": "mem://tmp/_id",
		"cache_url":      "mem://tcache",
	}
	conf, err := config.Load(cfg, "")
	assert.Nil(t, err)
	assert.NotNil(t, conf)

	ctx := context.Background()
	rep, err := New(ctx, conf)
	assert.Nil(t, err)
	assert.NotNil(t, rep)
	assert.Equal(t, defaultExpiration, rep.CacheExpiration)

	for i := 0; i < 20; i++ {
		tmp := &Person{
			Name: "test" + fmt.Sprintf("%v", i),
			Age:  18 + i,
		}

		err = rep.Create(ctx, tmp)
		assert.Nil(t, err)
	}

	var out []*Person
	err = rep.Find(ctx, &QueryOpt{}, &out)
	assert.Nil(t, err)
	assert.Equal(t, 20, len(out))

	var lout []*Person
	err = rep.Find(ctx, &QueryOpt{Limit: 10}, &lout)
	assert.Nil(t, err)
	assert.Equal(t, 10, len(lout))

	var sout []*Person
	err = rep.Find(ctx, &QueryOpt{Limit: 5, Page: 1}, &sout)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(sout))

}
