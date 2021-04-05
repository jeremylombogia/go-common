package event

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/imdario/mergo"
	"github.com/sahalazain/go-common/config"
	"github.com/sahalazain/go-common/logger"
	"gocloud.dev/docstore"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/kafkapubsub"
)

//Hybrid hybrid outbox pubsub repository
type Hybrid struct {
	CollectionURL string      `json:"collection_url,omitempty" mapstructure:"collection_url"`
	CacheURL      string      `json:"cache_url,omitempty" mapstructure:"cache_url"`
	Config        EventConfig `json:"config,omitempty" mapstructure:"config"`
	PubsubURL     string      `json:"pubsub_url,omitempty" mapstructure:"pubsub_url"`
	KafkaBroker   string      `json:"kafka_broker,omitempty" mapstructure:"kafka_broker"`
	collection    *docstore.Collection
	topics        map[string]*pubsub.Topic
	channel       chan *OutboxRecord
	ec            *EmitterCache
}

//NewHybridEmitter create instance of hybrid emitter
func NewHybridEmitter(ctx context.Context, conf config.Getter) (*Hybrid, error) {

	var hc Hybrid

	if err := conf.Unmarshal(&hc); err != nil {
		return nil, err
	}

	if hc.CollectionURL == "" {
		return nil, errors.New("missing collection_url param")
	}

	if hc.CacheURL == "" {
		return nil, errors.New("missing cache_url param")
	}

	if hc.PubsubURL == "" {
		return nil, errors.New("missing pubsub_url param")
	}

	col, err := docstore.OpenCollection(ctx, hc.CollectionURL)
	if err != nil {
		return nil, err
	}

	hc.collection = col

	ec, err := NewEmitterCache(hc.CacheURL)
	if err != nil {
		return nil, err
	}

	hc.ec = ec

	hc.topics = make(map[string]*pubsub.Topic)
	hc.channel = make(chan *OutboxRecord)

	go hc.sender(ctx)
	return &hc, nil
}

// Push publish sequential event
func (h *Hybrid) Push(ctx context.Context, event, key string, message interface{}, metadata map[string]interface{}) error {
	return h.send(ctx, event, key, message, metadata)
}

//Publish publish message
func (h *Hybrid) Publish(ctx context.Context, event string, message interface{}, metadata map[string]interface{}) error {
	return h.send(ctx, event, "", message, metadata)
}

func (h *Hybrid) send(ctx context.Context, event, key string, message interface{}, metadata map[string]interface{}) error {
	event = h.Config.getTopic(event)
	md := h.Config.getMetadata(event)

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
		md["previous"] = h.ec.getPrevious(ctx, event+key)
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

	if err := h.collection.Get(ctx, ob); err != nil {
		code := gcerrors.Code(err)
		if code != gcerrors.NotFound {
			return err
		}
	}

	if !ob.CreatedAt.IsZero() {
		return nil
	}

	ob.CreatedAt = time.Now()

	if err := h.collection.Create(ctx, ob); err != nil {
		return err
	}

	if seq {
		h.ec.setCurrent(ctx, event+key, mhash)
	} else {
		h.channel <- ob
	}

	return nil
}

func (h *Hybrid) sender(ctx context.Context) error {
	log := logger.GetLoggerContext(ctx, "event", "hybridSender")
	if h.topics == nil {
		log.Error("Hybrid emitter is not configured")
		return errors.New("Hybrid emitter is not configured")
	}

	for o := range h.channel {

		if _, ok := h.topics[o.KafkaTopic]; !ok {

			to := strings.ReplaceAll(h.PubsubURL, "$TOPIC", o.KafkaTopic)

			u, err := url.Parse(to)
			if err != nil {
				return err
			}

			if u.Scheme == "kafka" {
				brokers := h.KafkaBroker
				if strings.Contains(u.Host, ":") {
					brokers = u.Host
				}

				if brokers == "" {
					return errors.New("missing kafka broker")
				}

				topic, err := kafkapubsub.OpenTopic(strings.Split(brokers, ","), kafkapubsub.MinimalConfig(), o.KafkaTopic, &kafkapubsub.TopicOptions{KeyName: "key"})
				if err != nil {
					return err
				}
				h.topics[o.KafkaTopic] = topic
			} else {
				topic, err := pubsub.OpenTopic(ctx, to)
				if err != nil {
					return err
				}
				h.topics[o.KafkaTopic] = topic
			}
		}

		t := h.topics[o.KafkaTopic]

		//k := sha256.Sum256(b)
		msg := &pubsub.Message{
			Body: []byte(o.KafkaValue),
			Metadata: map[string]string{
				"key": o.KafkaKey,
			},
		}

		if err := t.Send(ctx, msg); err != nil {
			log.WithError(err).WithField("topic", o.KafkaTopic).WithField("message", o.KafkaValue).Error("Error sending event")
			continue
		}

		if err := h.collection.Delete(ctx, o); err != nil {
			log.WithError(err).WithField("topic", o.KafkaTopic).WithField("id", o.ID).Error("Error deleting event")
			continue
		}

	}
	return nil
}
