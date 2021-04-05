package event

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
)

//OutboxRecord outbox model
type OutboxRecord struct {
	ID         string    `json:"_id,omitempty" mapstructure:"_id" docstore:"_id"`
	GroupID    string    `json:"group_id,omitempty" mapstructure:"group_id" docstore:"group_id"`
	KafkaTopic string    `json:"kafka_topic,omitempty" mapstructure:"kafka_topic" docstore:"kafka_topic"`
	KafkaKey   string    `json:"kafka_key,omitempty" mapstructure:"kafka_key" docstore:"kafka_key"`
	KafkaValue string    `json:"kafka_value,omitempty" mapstructure:"kafka_value" docstore:"kafka_value"`
	CreatedAt  time.Time `json:"created_at,omitempty" mapstructure:"created_at" docstore:"created_at"`
}

//Hash calculate request hash
func (o *OutboxRecord) Hash() []byte {
	val := fmt.Sprintf("%v", o)
	h := sha256.Sum256([]byte(val))
	return h[:]
}

// GenerateID generate record ID
func (o *OutboxRecord) GenerateID() *OutboxRecord {
	h := o.Hash()
	o.ID = base64.StdEncoding.EncodeToString(h[:])
	return o
}
