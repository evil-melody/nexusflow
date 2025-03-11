package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	types "github.com/evil-melody/nexusflow/types"
	utils "github.com/evil-melody/nexusflow/utils"
	"github.com/go-redis/redis/v8"
)

// RedisStorage 增强
type RedisStorage struct {
	client       *redis.Client
	duplicateSet string
	mu           sync.Mutex
}

func NewRedisStorage(addr string, duplicateSet string) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return &RedisStorage{
		client:       client,
		duplicateSet: duplicateSet,
	}, nil
}

func (r *RedisStorage) Enqueue(task *types.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	hash := types.HashTask(task)
	if exists, _ := r.client.SIsMember(context.Background(), r.duplicateSet, hash).Result(); exists {
		return errors.New("duplicate task")
	}
	if err := r.client.SAdd(context.Background(), r.duplicateSet, hash).Err(); err != nil {
		return err
	}

	queueKey := fmt.Sprintf("nf:%s:queue", task.Group)
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return r.client.RPush(context.Background(), queueKey, taskJSON).Err()
}

func (r *RedisStorage) Dequeue(group string) (*types.Task, error) {
	queueKey := fmt.Sprintf("nf:%s:queue", group)
	taskJSON, err := r.client.LPop(context.Background(), queueKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, utils.ErrQueueEmpty
		}
		return nil, err
	}

	var task types.Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (r *RedisStorage) Lock(taskID string) bool {
	return r.client.SetNX(context.Background(), fmt.Sprintf("nf:lock:%s", taskID), "locked", 10*time.Second).Val()
}

func (r *RedisStorage) Unlock(taskID string) bool {
	return r.client.Del(context.Background(), fmt.Sprintf("nf:lock:%s", taskID)).Val() > 0
}

func (r *RedisStorage) IsDuplicate(task *types.Task) bool {
	hash := types.HashTask(task)
	return r.client.SIsMember(context.Background(), r.duplicateSet, hash).Val()
}

func (r *RedisStorage) UpdateStatus(taskID string, status types.Status) error {
	task, err := r.GetTask(taskID)
	if err != nil {
		return err
	}
	task.Status = status
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return r.client.Set(context.Background(), fmt.Sprintf("nf:task:%s", taskID), taskJSON, 0).Err()
}

func (r *RedisStorage) GetTask(taskID string) (*types.Task, error) {
	taskJSON, err := r.client.Get(context.Background(), fmt.Sprintf("nf:task:%s", taskID)).Result()
	if err != nil {
		return nil, err
	}
	var task types.Task
	if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
