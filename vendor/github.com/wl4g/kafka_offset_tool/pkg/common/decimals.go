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
	"math"
)

/**
 * Trunc 2bit decimal.
 */
func DecimalTrunc2b(value float64) float64 {
	//value, _ = strconv.ParseFloat(fmt.Sprintf("%.2f", value), 32)
	// 0.5 is to calculate rounding. If you want to keep a few decimal places, you can change 3.
	return math.Trunc(value*1e3+0.5) * 1e-3
}
