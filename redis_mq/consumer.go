package redis_mq

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const (
	listSuffix, zsetSuffix = ":list", ":zset"
)

type consumer struct {
	once            sync.Once
	redisCmd        redis.Cmdable
	ctx             context.Context
	topicName       string
	handler         Handler
	rateLimitPeriod time.Duration
	options         ConsumerOptions
	_               struct{}
}

// ConsumerOptions ...
type ConsumerOptions struct {
	RateLimitPeriod time.Duration
	UseBLPop        bool
}

// ConsumerOption ...
type ConsumerOption func(options *ConsumerOptions)

// NewRateLimitPeriod ...
func NewRateLimitPeriod(d time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.RateLimitPeriod = d
	}
}

// UseBLPop ...
func UseBLPop(u bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.UseBLPop = u
	}
}

// Consumer ...
type Consumer = *consumer

// NewMQConsumer ...
func NewMQConsumer(ctx context.Context, redisCmd redis.Cmdable, topicName string, opts ...ConsumerOption) Consumer {
	consumer := &consumer{
		redisCmd:  redisCmd,
		ctx:       ctx,
		topicName: topicName,
	}
	for _, o := range opts {
		o(&consumer.options)
	}
	if consumer.options.RateLimitPeriod == 0 {
		consumer.options.RateLimitPeriod = time.Microsecond * 200
	}
	return consumer
}

// SetHandler ...
func (s *consumer) SetHandler(handler Handler) {
	s.once.Do(func() {
		s.startGetListMessage()
		s.startGetDelayMessage()
	})
	s.handler = handler
}

func (s *consumer) startGetListMessage() {
	go func() {
		ticker := time.NewTicker(s.options.RateLimitPeriod)
		defer func() {
			log.Println("stop get list message.")
			ticker.Stop()
		}()
		topicName := s.topicName + listSuffix
		for {
			select {
			case <-s.ctx.Done():
				log.Printf("context Done msg: %#v \n", s.ctx.Err())
				return
			case <-ticker.C:
				var revBody []byte
				var err error
				if !s.options.UseBLPop {
					revBody, err = s.redisCmd.LPop(topicName).Bytes()
				} else {
					revs := s.redisCmd.BLPop(time.Second, topicName)
					err = revs.Err()
					revValues := revs.Val()
					if len(revValues) >= 2 {
						revBody = []byte(revValues[1])
					}
				}
				if err == redis.Nil {
					continue
				}
				if err != nil {
					log.Printf("LPOP error: %#v \n", err)
					continue
				}

				if len(revBody) == 0 {
					continue
				}
				msg := &Message{}
				json.Unmarshal(revBody, msg)
				if s.handler != nil {
					s.handler.HandleMessage(msg)
				}
			}
		}
	}()
}

func (s *consumer) startGetDelayMessage() {
	go func() {
		ticker := time.NewTicker(s.options.RateLimitPeriod)
		defer func() {
			log.Println("stop get delay message.")
			ticker.Stop()
		}()
		topicName := s.topicName + zsetSuffix
		for {
			currentTime := time.Now().Unix()
			select {
			case <-s.ctx.Done():
				log.Printf("context Done msg: %#v \n", s.ctx.Err())
				return
			case <-ticker.C:
				var valuesCmd *redis.ZSliceCmd
				_, err := s.redisCmd.TxPipelined(func(pip redis.Pipeliner) error {
					valuesCmd = pip.ZRangeWithScores(topicName, 0, currentTime)
					pip.ZRemRangeByScore(topicName, "0", strconv.FormatInt(currentTime, 10))
					return nil
				})
				if err != nil {
					log.Printf("zset pip error: %#v \n", err)
					continue
				}
				rev := valuesCmd.Val()
				for _, revBody := range rev {
					msg := &Message{}
					json.Unmarshal([]byte(revBody.Member.(string)), msg)
					if s.handler != nil {
						s.handler.HandleMessage(msg)
					}
				}
			}
		}
	}()
}
