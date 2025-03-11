/*
 * @Author: evil-melody
 * @Date: 2025-03-11 16:49:19
 * @LastEditors: evil-melody
 * @LastEditTime: 2025-03-11 16:55:44
 * @FilePath: /nexusflow/types/task.go
 * @Description:
 *
 * Copyright (c) 2025 by emgai, All Rights Reserved.
 */
package types

import (
	"fmt"
	"hash/fnv"
	"time"
)

// Task 结构体增强
type Task struct {
	NexusFlowId string    `json:"nexus_flow_id"`
	Payload     []byte    `json:"payload"`
	CreateAt    time.Time `json:"create_at"`
	Group       string    `json:"group"`
	Status      Status    `json:"status"`
	RetryCount  int       `json:"retry_count"`
	LastError   string    `json:"last_error"`
}

// 任务状态枚举
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSuccess    Status = "success"
	StatusFailed     Status = "failed"
)

// 计算任务哈希值
func HashTask(task *Task) string {
	h := fnv.New32a()
	h.Write(task.Payload)
	return fmt.Sprintf("%x", h.Sum32())
}
