package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	types "github.com/evil-melody/nexusflow/types"
)

// RocketMQ 存储实现
type RocketMQStorage struct {
	producer       rocketmq.Producer
	consumer       rocketmq.PushConsumer
	duplicateMap   map[string]bool
	mu             sync.Mutex
	groupConsumers map[string]rocketmq.PushConsumer
}

func NewRocketMQStorage(nameServer []string, topic string) (*RocketMQStorage, error) {
	p, err := rocketmq.NewProducer(
		producer.WithGroupName("testGroup"),
		producer.WithNsResolver(primitive.NewPassthroughResolver(nameServer)),
	)
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}

	rs := &RocketMQStorage{
		producer:       p,
		duplicateMap:   make(map[string]bool),
		groupConsumers: make(map[string]rocketmq.PushConsumer),
	}
	return rs, nil
}

func (r *RocketMQStorage) Enqueue(task *types.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	hash := types.HashTask(task)
	if r.duplicateMap[hash] {
		return errors.New("duplicate task")
	}
	r.duplicateMap[hash] = true

	msg := &primitive.Message{
		Topic: "task_topic",
		Body:  task.Payload,
	}
	msg.WithProperty("task_id", task.ID)
	msg.WithProperty("group", task.Group)

	_, err := r.producer.SendSync(context.Background(), msg)
	return err
}

func (r *RocketMQStorage) Dequeue(group string, nameServer []string) (*types.Task, error) {
	if _, ok := r.groupConsumers[group]; !ok {
		c, err := rocketmq.NewPushConsumer(
			consumer.WithGroupName(fmt.Sprintf("consumer_group_%s", group)),
			consumer.WithNsResolver(primitive.NewPassthroughResolver(nameServer)),
		)
		if err != nil {
			return nil, err
		}
		// 创建一个通道用于接收任务
		taskChan := make(chan *types.Task, 1)
		done := make(chan struct{})

		if err := c.Subscribe("task_topic", consumer.MessageSelector{}, func(ctx context.Context,
			msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, msg := range msgs {
				if msg.GetProperty("group") == group {
					task := &types.Task{
						ID:      msg.GetProperty("task_id"),
						Payload: msg.Body,
						Group:   group,
					}
					// 将任务发送到通道中
					taskChan <- task
					close(done)
					return consumer.ConsumeSuccess, nil
				}
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
			return nil, err
		}
		if err := c.Start(); err != nil {
			return nil, err
		}
		r.groupConsumers[group] = c

		// 等待一段时间获取任务
		select {
		case task := <-taskChan:
			return task, nil
		case <-time.After(5 * time.Second):
			return nil, errors.New("no task available")
		}
	}

	return nil, errors.New("no task available")
}

func (r *RocketMQStorage) Lock(taskID string) bool {
	// 简单模拟加锁
	return true
}

func (r *RocketMQStorage) Unlock(taskID string) bool {
	// 简单模拟解锁
	return true
}

func (r *RocketMQStorage) IsDuplicate(task *types.Task) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	hash := types.HashTask(task)
	return r.duplicateMap[hash]
}
