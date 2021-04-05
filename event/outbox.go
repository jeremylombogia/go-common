package event

import (
	"context"
	"errors"
	"time"

	"github.com/imdario/mergo"
	"github.com/sahalazain/go-common/config"
	"gocloud.dev/docstore"
	"gocloud.dev/gcerrors"
)

//Outbox outbox repository
type Outbox struct {
	CollectionURL string      `json:"collection_url,omitempty" mapstructure:"collection_url"`
	CacheURL      string      `json:"cache_url,omitempty" mapstructure:"cache_url"`
	Config        EventConfig `json:"config,omitempty" mapstructure:"config"`
	collection    *docstore.Collection
	ec            *EmitterCache
}

//NewOutboxEmitter create outbox emitter instance
func NewOutboxEmitter(ctx context.Context, conf config.Getter) (*Outbox, error) {

	var ob Outbox
	if err := conf.Unmarshal(&ob); err != nil {
		return nil, err
	}

	if ob.CollectionURL == "" {
		return nil, errors.New("missing collection_url param")
	}

	if ob.CacheURL == "" {
		return nil, errors.New("missing cache_url param")
	}

	col, err := docstore.OpenCollection(ctx, ob.CollectionURL)
	if err != nil {
		return nil, err
	}

	ob.collection = col

	ec, err := NewEmitterCache(ob.CacheURL)
	if err != nil {
		return nil, err
	}

	ob.ec = ec

	return &ob, nil
}

//Publish store message to outbox
func (o *Outbox) Publish(ctx context.Context, event string, message interface{}, metadata map[string]interface{}) error {
	return o.send(ctx, event, "", message, metadata)
}

func (o *Outbox) send(ctx context.Context, event, key string, message interface{}, metadata map[string]interface{}) error {
	event = o.Config.getTopic(event)
	md := o.Config.getMetadata(event)

	if metadata != nil {
		if err := mergo.Merge(&md, metadata); err != nil {
			return err
		}
	}

	mhash, err := hash(message)
	if err != nil {
		return err
	}

	md["hash"] = mhash

	seq := false

	if key == "" {
		key = mhash
	} else {
		seq = true
		md["previous"] = o.ec.getPrevious(ctx, event+key)
	}

	msg := &EventMessage{
		Data:     message,
		Metadata: md,
	}

	b, err := msg.ToBytes()
	if err != nil {
		return err
	}

	ob := (&OutboxRecord{
		KafkaKey:   key,
		KafkaTopic: event,
		KafkaValue: string(b),
	}).GenerateID()

	if err := o.collection.Get(ctx, ob); err != nil {
		code := gcerrors.Code(err)
		if code != gcerrors.NotFound {
			return err
		}
	}

	if !ob.CreatedAt.IsZero() {
		return nil
	}

	ob.CreatedAt = time.Now()

	if err := o.collection.Create(ctx, ob); err != nil {
		return err
	}

	if seq {
		o.ec.setCurrent(ctx, event+key, mhash)
	}

	return nil
}

// Push publish sequential event
func (o *Outbox) Push(ctx context.Context, event, key string, message interface{}, metadata map[string]interface{}) error {
	return o.send(ctx, event, key, message, metadata)
}
