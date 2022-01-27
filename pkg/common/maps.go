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
	"errors"
	"reflect"
	"sort"
)

// To input map keys ordered.
func ToOrderedKeys(inputMap interface{}) ([]string, error) {
	r := reflect.ValueOf(inputMap)
	if r.Kind() != reflect.Map {
		return nil, errors.New("Invalid input type must compatibled to map<string>interface{}.")
	}
	keys := make([]string, 0, len(r.MapKeys()))
	refKeys := r.MapKeys()
	for _, key := range refKeys {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	return keys, nil
}

// To input map keys ordered.
func ToOrderedKeysInt(inputMap interface{}) ([]int, error) {
	r := reflect.ValueOf(inputMap)
	if r.Kind() != reflect.Map {
		return nil, errors.New("Invalid input type must compatibled to map<int>interface{}.")
	}
	keys := make([]int, 0, len(r.MapKeys()))
	refKeys := r.MapKeys()
	for _, key := range refKeys {
		keys = append(keys, int(key.Int()))
	}
	sort.Ints(keys)
	return keys, nil
}
