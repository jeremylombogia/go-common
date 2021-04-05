package event

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/imdario/mergo"
	"github.com/sahalazain/go-common/config"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/kafkapubsub"
)

//PubSub pubsub event emitter
type PubSub struct {
	Config      EventConfig `json:"config,omitempty" mapstructure:"config"`
	CacheURL    string      `json:"cache_url,omitempty" mapstructure:"cache_url"`
	PubsubURL   string      `json:"pubsub_url,omitempty" mapstructure:"pubsub_url"`
	KafkaBroker string      `json:"kafka_broker,omitempty" mapstructure:"kafka_broker"`
	topics      map[string]*pubsub.Topic
	ec          *EmitterCache
}

//NewPubSubEmitter create instance of pubsub emitter
func NewPubSubEmitter(ctx context.Context, conf config.Getter) (*PubSub, error) {
	var ps PubSub

	if err := conf.Unmarshal(&ps); err != nil {
		return nil, err
	}

	if ps.PubsubURL == "" {
		return nil, errors.New("missing pubsub_url param")
	}

	ec, err := NewEmitterCache(ps.CacheURL)
	if err != nil {
		return nil, err
	}

	ps.ec = ec

	ps.topics = make(map[string]*pubsub.Topic)
	return &ps, nil
}

//Publish publish message
func (p *PubSub) Publish(ctx context.Context, event string, message interface{}, metadata map[string]interface{}) error {
	return p.send(ctx, event, "", message, metadata)
}

// Push publish sequential event
func (p *PubSub) Push(ctx context.Context, event string, key string, message interface{}, metadata map[string]interface{}) error {
	return p.send(ctx, event, key, message, metadata)
}

func (p *PubSub) send(ctx context.Context, event, key string, message interface{}, metadata map[string]interface{}) error {
	if p.topics == nil {
		return errors.New("pubsub is not configured")
	}

	event = p.Config.getTopic(event)

	if _, ok := p.topics[event]; !ok {

		to := strings.ReplaceAll(p.PubsubURL, "$TOPIC", event)

		u, err := url.Parse(to)
		if err != nil {
			return err
		}

		if u.Scheme == "kafka" {
			brokers := p.KafkaBroker
			if strings.Contains(u.Host, ":") {
				brokers = u.Host
			}

			if brokers == "" {
				return errors.New("missing kafka broker")
			}

			topic, err := kafkapubsub.OpenTopic(strings.Split(brokers, ","), kafkapubsub.MinimalConfig(), event, &kafkapubsub.TopicOptions{KeyName: "key"})
			if err != nil {
				return err
			}
			p.topics[event] = topic
		} else {
			topic, err := pubsub.OpenTopic(ctx, to)
			if err != nil {
				return err
			}
			p.topics[event] = topic
		}

	}

	t := p.topics[event]

	md := p.Config.getMetadata(event)

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
		md["previous"] = p.ec.getPrevious(ctx, event+key)
	}

	msg := &EventMessage{
		Data:     message,
		Metadata: md,
	}

	b, err := msg.ToBytes()
	if err != nil {
		return err
	}

	pmsg := &pubsub.Message{
		Body: b,
		Metadata: map[string]string{
			"key": key,
		},
	}
	if err := t.Send(ctx, pmsg); err != nil {
		return err
	}

	if seq {
		p.ec.setCurrent(ctx, event+key, mhash)
	}

	return nil

}
