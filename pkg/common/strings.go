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
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

// Check whether the string contains
func StringsContains(values []string, search string, ignoreCase bool) bool {
	for i := 0; i < len(values); i++ {
		s1 := strings.TrimSpace(values[i])
		s2 := strings.TrimSpace(search)
		if ignoreCase && strings.EqualFold(s1, s2) {
			return true
		} else if s1 == s2 {
			return true
		}
	}
	return false
}

// Is any blank
func IsAnyBlank(values ...string) bool {
	if values == nil || len(values) <= 0 {
		return true
	}
	for _, value := range values {
		if IsBlank(value) {
			return true
		}
	}
	return false
}

// Is blank
func IsBlank(value string) bool {
	return &value == nil || strings.TrimSpace(value) == "" || len(strings.TrimSpace(value)) <= 0
}

/**
 * String regular expression match.
 */
func Match(regex string, value string) bool {
	if IsBlank(regex) || IsBlank(value) {
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
