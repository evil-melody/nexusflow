/*
 * @Author: evil-melody
 * @Date: 2025-03-11 16:03:24
 * @LastEditors: evil-melody
 * @LastEditTime: 2025-03-11 16:08:24
 * @FilePath: /nexusflow/utils/string.go
 * @Description:
 *
 * Copyright (c) 2025 by emgai, All Rights Reserved.
 */
package utils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

/**
 * Func:  GenerateToken 生成基于用户ID、时间、组织和盐值的token
 *
 * @author evil-melody
 *
 * @params ...
 * @return
 */
func GenerateToken(ids, secretKey string, additionalSalt ...string) (string, error) {
	// 创建一个时间戳
	timestamp := time.Now().Unix()

	// 将用户ID、时间戳、组织和额外的盐值连接成一个字符串
	var dataToSign []byte
	dataToSign = append(dataToSign, ids...)
	dataToSign = append(dataToSign, strconv.FormatInt(timestamp, 10)...)
	for _, salt := range additionalSalt {
		dataToSign = append(dataToSign, salt...)
	}

	// 创建一个新的HMAC哈希
	mac := hmac.New(sha256.New, []byte(secretKey))

	// 写入数据
	_, err := mac.Write(dataToSign)
	if err != nil {
		return "", err
	}

	// 计算HMAC哈希
	hash := mac.Sum(nil)

	// 将时间戳和HMAC哈希编码为base64字符串
	token := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d.%x", timestamp, hash)))

	return token, nil
}

// EncodeBase62 将整数编码为 Base62 字符串
func EncodeBase62(num uint64) string {
	if num == 0 {
		return string(base62Chars[0])
	}
	var encoded []byte
	for num > 0 {
		remainder := num % 62
		encoded = append([]byte{base62Chars[remainder]}, encoded...)
		num /= 62
	}
	return string(encoded)
}

// GenerateUniqueShortCode 生成唯一短码字符串
func GenerateUniqueShortCode() string {
	// 使用 UUID 作为唯一 ID
	id := uuid.New()
	// 将 UUID 的高 64 位和低 64 位组合成一个 128 位整数
	high := uint64(id[0])<<56 | uint64(id[1])<<48 | uint64(id[2])<<40 | uint64(id[3])<<32 |
		uint64(id[4])<<24 | uint64(id[5])<<16 | uint64(id[6])<<8 | uint64(id[7])
	low := uint64(id[8])<<56 | uint64(id[9])<<48 | uint64(id[10])<<40 | uint64(id[11])<<32 |
		uint64(id[12])<<24 | uint64(id[13])<<16 | uint64(id[14])<<8 | uint64(id[15])

	// 这里简单处理，先编码高 64 位，再编码低 64 位
	highEncoded := EncodeBase62(high)
	lowEncoded := EncodeBase62(low)
	return highEncoded + lowEncoded
}
