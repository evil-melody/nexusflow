package nexusflow

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/evil-melody/nexusflow/types"
	"github.com/evil-melody/nexusflow/utils"
)

// Storage 接口增强
type Storage interface {
	Enqueue(task *utils.Task) error
	Dequeue(group string) (*utils.Task, error)
	Lock(taskID string) bool
	Unlock(taskID string) bool
	IsDuplicate(task *utils.Task) bool
	UpdateStatus(taskID string, status utils.Status) error
	GetTask(taskID string) (*utils.Task, error)
}

// TaskManager 增强
type TaskManager struct {
	storage    Storage
	producers  map[string]func() (*utils.Task, error)
	consumers  map[string]func(task *utils.Task) error
	checkers   map[string]func(task *utils.Task) bool
	wg         sync.WaitGroup
	stopCh     chan struct{}
	producerWg sync.WaitGroup
	consumerWg sync.WaitGroup
}

// NewTaskManager 创建任务管理器
func NewTaskManager(storage Storage) *TaskManager {
	return &TaskManager{
		storage:   storage,
		producers: make(map[string]func() (*utils.Task, error)),
		consumers: make(map[string]func(task *utils.Task) error),
		checkers:  make(map[string]func(task *utils.Task) bool),
		stopCh:    make(chan struct{}),
	}
}

// RegisterChecker 注册对账检查器
func (tm *TaskManager) RegisterChecker(group string, checker func(task *utils.Task) bool) {
	tm.checkers[group] = checker
}

// StartProducers 启动生产者
func (tm *TaskManager) StartProducers() {
	tm.producerWg.Add(len(tm.producers))
	for group, producer := range tm.producers {
		go func(g string, p func() (*utils.Task, error)) {
			defer tm.producerWg.Done()
			for {
				select {
				case <-tm.stopCh:
					return
				default:
					func() {
						defer func() {
							if r := recover(); r != nil {
								fmt.Printf("Producer panic in group %s: %v\n", g, r)
							}
						}()
						task, err := p()
						if err != nil {
							fmt.Printf("Producer error in group %s: %v\n", g, err)
							return
						}
						task.NexusFlowId = utils.GenerateToken()
						task.CreateAt = time.Now()
						task.Group = g
						task.Status = utils.StatusPending

						if tm.storage.IsDuplicate(task) {
							fmt.Printf("Duplicate task skipped: %s\n", task.NexusFlowId)
							return
						}

						if err := tm.storage.Enqueue(task); err != nil {
							fmt.Printf("Enqueue failed for task %s: %v\n", task.NexusFlowId, err)
						}
					}()
				}
			}
		}(group, producer)
	}
}

// StartConsumers 启动消费者
func (tm *TaskManager) StartConsumers() {
	tm.consumerWg.Add(len(tm.consumers))
	for group, consumer := range tm.consumers {
		go func(g string, c func(task *utils.Task) error) {
			defer tm.consumerWg.Done()
			for {
				select {
				case <-tm.stopCh:
					return
				default:
					func() {
						defer func() {
							if r := recover(); r != nil {
								fmt.Printf("Consumer panic in group %s: %v\n", g, r)
							}
						}()
						task, err := tm.storage.Dequeue(g)
						if err != nil {
							if errors.Is(err, utils.ErrQueueEmpty) {
								time.Sleep(1 * time.Second)
								return
							}
							fmt.Printf("Dequeue error in group %s: %v\n", g, err)
							return
						}

						if !tm.storage.Lock(task.ID) {
							fmt.Printf("Lock failed for task %s\n", task.ID)
							return
						}

						task.Status = types.StatusProcessing
						if err := tm.storage.UpdateStatus(task.ID, types.StatusProcessing); err != nil {
							fmt.Printf("Update status failed for task %s: %v\n", task.ID, err)
							tm.storage.Unlock(task.ID)
							return
						}

						if err := c(task); err != nil {
							task.LastError = err.Error()
							task.RetryCount++
							task.Status = types.StatusFailed
							if err := tm.storage.UpdateStatus(task.ID, types.StatusFailed); err != nil {
								fmt.Printf("Update status failed for task %s: %v\n", task.ID, err)
							}
							tm.storage.Unlock(task.ID)
							return
						}

						task.Status = types.StatusSuccess
						if err := tm.storage.UpdateStatus(task.ID, types.StatusSuccess); err != nil {
							fmt.Printf("Update status failed for task %s: %v\n", task.ID, err)
						}
						tm.storage.Unlock(task.ID)
					}()
				}
			}
		}(group, consumer)
	}
}

// StartChecker 启动对账检查器
func (tm *TaskManager) StartChecker() {
	tm.wg.Add(1)
	go func() {
		defer tm.wg.Done()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-tm.stopCh:
				return
			case <-ticker.C:
				for group, checker := range tm.checkers {
					for {
						task, err := tm.storage.Dequeue(group)
						if err != nil {
							if errors.Is(err, utils.ErrQueueEmpty) {
								break
							}
							fmt.Printf("Checker error in group %s: %v\n", group, err)
							break
						}

						if checker(task) {
							fmt.Printf("Task %s passed check\n", task.ID)
							task.Status = types.StatusSuccess
							if err := tm.storage.UpdateStatus(task.ID, types.StatusSuccess); err != nil {
								fmt.Printf("Update status failed for task %s: %v\n", task.ID, err)
							}
						} else {
							fmt.Printf("Task %s failed check, retrying\n", task.ID)
							task.RetryCount++
							task.Status = types.StatusPending
							if err := tm.storage.Enqueue(task); err != nil {
								fmt.Printf("Retry enqueue failed for task %s: %v\n", task.ID, err)
							}
						}
					}
				}
			}
		}
	}()
}

// Stop 停止所有组件
func (tm *TaskManager) Stop() {
	close(tm.stopCh)
	tm.producerWg.Wait()
	tm.consumerWg.Wait()
	tm.wg.Wait()
}
