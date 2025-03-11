/*
 * @Author: evil-melody
 * @Date: 2025-03-11 16:51:10
 * @LastEditors: evil-melody
 * @LastEditTime: 2025-03-11 16:51:24
 * @FilePath: /nexusflow/handler/check.go
 * @Description:
 *
 * Copyright (c) 2025 by emgai, All Rights Reserved.
 */
package handler

import (
	"errors"
	"time"

	types "github.com/evil-melody/nexusflow/types"
)

// ErrQueueEmpty 队列空错误
var ErrQueueEmpty = errors.New("queue is empty")

// 默认对账检查器
func DefaultChecker(task *types.Task) bool {
	return task.RetryCount < 3 && time.Since(task.CreateAt) < 24*time.Hour
}
