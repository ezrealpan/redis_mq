package redis_mq

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

//Message ...
type Message struct {
	ID        string `json:"id"`
	Body      []byte `json:"body"`
	Timestamp int64  `json:"timestamp"`
	DelayTime int64  `json:"delayTime"`
	_         struct{}
}

//NewMessage ...
func NewMessage(id string, body []byte) *Message {
	if id == "" {
		id = uuid.NewV4().String()
	}
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().Unix(),
		DelayTime: time.Now().Unix(),
	}
}

//Handler ...
type Handler interface {
	HandleMessage(msg *Message)
}
