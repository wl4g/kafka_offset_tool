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
	"fmt"
	"log"
	"os"
	"strconv"
)

var kfDebug = false

func init() {
	kfDebug, _ = strconv.ParseBool(os.Getenv("KF_DEBUG"))
}

/**
 * Print error message, If it is a fatal exception, it exits(2) the process.
 */
func FatalExit(errMsgFormat string, args ...interface{}) {
	if kfDebug {
		log.Panicf(fmt.Sprintf(errMsgFormat, args))
	} else {
		log.Printf(fmt.Sprintf(errMsgFormat, args))
	}
	os.Exit(2)
}

/**
 * Print error message, If it is a fatal exception, it exits(2) the process.
 */
func ErrorExit(err error, errMsgFormat string, args ...interface{}) {
	if kfDebug {
		log.Panicf(fmt.Sprintf(errMsgFormat, args), err)
	} else {
		log.Printf(fmt.Sprintf(errMsgFormat, args), err.Error())
	}
	os.Exit(2)
}
