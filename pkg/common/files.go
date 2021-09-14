/**
 * Copyright 2017 ~ 2025 the original author or authors[983708408@qq.com].
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package common

import (
	"os"
)

/**
 * Write bytes to files.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func WriteFile(path string, data []byte, append bool) error {
	if !Exists(path) {
		os.Create(path)
	}
	if append {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		file.Write(data)
		defer file.Close()
	} else {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		file.Write(data)
		defer file.Close()
	}
	return nil
}

/**
 * Determine whether the given path file/folder exists.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

/**
 * Determine whether the given path is a folder.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

/**
 * Determine whether the given path is a file.
 * @author Wang.sir <wanglsir@gmail.com,983708408@qq.com>
 * @date 19-07-22
 */
func IsFile(path string) bool {
	return !IsDir(path)
}
