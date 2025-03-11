package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	ptypes "github.com/evil-melody/nexusflow/types"
	putils "github.com/evil-melody/nexusflow/utils"
)

// 本地磁盘存储实现
type LocalDiskStorage struct {
	baseDir        string
	duplicateMap   map[string]bool
	queueFilePaths map[string]string
	mu             sync.Mutex
}

func NewLocalDiskStorage(baseDir string) (*LocalDiskStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &LocalDiskStorage{
		baseDir:        baseDir,
		duplicateMap:   make(map[string]bool),
		queueFilePaths: make(map[string]string),
	}, nil
}

func (l *LocalDiskStorage) getQueueFilePath(group string) string {
	if path, ok := l.queueFilePaths[group]; ok {
		return path
	}
	path := filepath.Join(l.baseDir, fmt.Sprintf("%s_queue.json", group))
	l.queueFilePaths[group] = path
	return path
}

func (l *LocalDiskStorage) Enqueue(task *putils.Task) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	hash := ptypes.HashTask(task)
	if l.duplicateMap[hash] {
		return errors.New("duplicate task")
	}
	l.duplicateMap[hash] = true

	filePath := l.getQueueFilePath(task.Group)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(task); err != nil {
		return err
	}
	return nil
}

func (l *LocalDiskStorage) Dequeue(group string) (*putils.Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	filePath := l.getQueueFilePath(group)
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("queue is empty")
		}
		return nil, err
	}
	defer file.Close()

	var tasks []*putils.Task
	decoder := json.NewDecoder(file)
	for {
		var task putils.Task
		if err := decoder.Decode(&task); err != nil {
			break
		}
		tasks = append(tasks, &task)
	}

	if len(tasks) == 0 {
		return nil, errors.New("queue is empty")
	}

	firstTask := tasks[0]
	tasks = tasks[1:]

	// 清空文件并重新写入剩余任务
	if err := file.Truncate(0); err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}

	for _, task := range tasks {
		encoder := json.NewEncoder(file)
		if err := encoder.Encode(task); err != nil {
			return nil, err
		}
	}

	return firstTask, nil
}

func (l *LocalDiskStorage) Lock(taskID string) bool {
	// 简单模拟加锁
	return true
}

func (l *LocalDiskStorage) Unlock(taskID string) bool {
	// 简单模拟解锁
	return true
}

func (l *LocalDiskStorage) IsDuplicate(task *putils.Task) bool {
	hash := ptypes.HashTask(task)
	return l.duplicateMap[hash]
}
