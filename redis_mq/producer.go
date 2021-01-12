package redis_mq

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/go-redis/redis"
)

// Producer ...
type Producer struct {
	redisCmd redis.Cmdable
	_        struct{}
}

// NewProducer ...
func NewProducer(cmd redis.Cmdable) *Producer {
	return &Producer{redisCmd: cmd}
}

// Publish ...
func (p *Producer) Publish(topicName string, body []byte) error {
	msg := NewMessage("", body)
	sendData, _ := json.Marshal(msg)
	return p.redisCmd.RPush(topicName+listSuffix, string(sendData)).Err()
}

// PublishDelayMsg ...
func (p *Producer) PublishDelayMsg(topicName string, body []byte, delay time.Duration) error {
	if delay <= 0 {
		return errors.New("delay need great than zero")
	}
	tm := time.Now().Add(delay)
	msg := NewMessage("", body)
	msg.DelayTime = tm.Unix()

	sendData, _ := json.Marshal(msg)
	return p.redisCmd.ZAdd(topicName+zsetSuffix, redis.Z{Score: float64(tm.Unix()), Member: string(sendData)}).Err()
}
