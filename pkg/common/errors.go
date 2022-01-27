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
func Warning(errMsgFormat string, args ...interface{}) {
	errMsg := errMsgFormat
	if args != nil && len(args) > 0 {
		errMsg = fmt.Sprintf(errMsgFormat, args...)
	}
	if kfDebug {
		log.Panicf("WARNING: " + errMsg)
	} else {
		log.Printf("WARNING: " + errMsg)
	}
}

/**
 * Print error message, If it is a fatal exception, it exits(2) the process.
 */
func FatalExit(errMsgFormat string, args ...interface{}) {
	errMsg := errMsgFormat
	if args != nil && len(args) > 0 {
		errMsg = fmt.Sprintf(errMsgFormat, args...)
	}
	if kfDebug {
		log.Panicf(errMsg)
	} else {
		log.Printf(errMsg)
	}
	os.Exit(2)
}

/**
 * Print error message, If it is a fatal exception, it exits(2) the process.
 */
func ErrorExit(err error, errMsgFormat string, args ...interface{}) {
	errMsg := errMsgFormat
	if args != nil && len(args) > 0 {
		errMsg = fmt.Sprintf(errMsgFormat, args...)
	}
	if kfDebug {
		log.Panicf(errMsg, err)
	} else {
		if err != nil {
			log.Printf(errMsg + " caused by: " + err.Error())
		} else {
			log.Printf(errMsg)
		}
	}
	os.Exit(2)
}
