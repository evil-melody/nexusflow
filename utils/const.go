/*
 * @Author: evil-melody
 * @Date: 2025-03-11 16:03:24
 * @LastEditors: evil-melody
 * @LastEditTime: 2025-03-11 16:31:41
 * @FilePath: /nexusflow/utils/const.go
 * @Description:
 *
 * Copyright (c) 2025 by emgai, All Rights Reserved.
 */
package utils

import "errors"

const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// ErrQueueEmpty 队列空错误
var ErrQueueEmpty = errors.New("queue is empty")
