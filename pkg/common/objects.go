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
	"fmt"
	jsoniter "github.com/json-iterator/go"
)

// To JSON string.
func ToJSONString(v interface{}) string {
	s, err := jsoniter.MarshalToString(v)
	if err != nil {
		fmt.Printf("Marshal data error! %s", err)
	}
	return s
}

// Deep objects copy.
func DeepCopy(value interface{}) interface{} {
	if valueMap, ok := value.(map[string]interface{}); ok {
		newMap := make(map[string]interface{})
		for k, v := range valueMap {
			newMap[k] = DeepCopy(v)
		}
		return newMap
	} else if valueSlice, ok := value.([]interface{}); ok {
		newSlice := make([]interface{}, len(valueSlice))
		for k, v := range valueSlice {
			newSlice[k] = DeepCopy(v)
		}

		return newSlice
	}
	return value
}

// Copy src to dst properties.
func CopyProperties(src interface{}, dst interface{}) {
	srcData, err := jsoniter.Marshal(src)
	if err != nil {
		panic(err)
	}
	jsoniter.Unmarshal(srcData, dst)

	//dstData, err := jsoniter.Marshal(dst)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("overlay:", string(dstData))
}

// Copy src properties to object.
func CopyObject(srcData []byte, dst interface{}) {
	jsoniter.Unmarshal(srcData, dst)
	//var dstData, err = jsoniter.Marshal(dst)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println("overlay:", string(dstData))
}
