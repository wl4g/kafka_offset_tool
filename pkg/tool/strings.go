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
package tool

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

// Check whether the string contains
func StringsContains(array []string, val string) bool {
	for i := 0; i < len(array); i++ {
		if strings.TrimSpace(array[i]) == strings.TrimSpace(val) {
			return true
		}
	}
	return false
}

// Is empty
func IsEmpty(str string) bool {
	return str == "" || len(str) <= 0
}

/**
 * String regular expression match.
 */
func Match(regex string, value string) bool {
	if IsEmpty(regex) || IsEmpty(value) {
		return false
	}
	if strings.TrimSpace(regex) == "*" {
		return true
	}
	match, err := regexp.Match(regex, []byte(value))
	if err != nil {
		panic(fmt.Sprintf("Invalid regular expression for %s", regex))
	}
	return match
}

/**
 * Format println result information.
 */
func PrintResult(title string, values []string) {
	buffer := bytes.Buffer{}
	for _, groupId := range values {
		buffer.WriteString(groupId)
		buffer.WriteString("\n")
	}
	fmt.Printf("\n======== %s ========\n%s", title, buffer.String())
}
