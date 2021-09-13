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
	"github.com/bndr/gotabulate"
)

/**
 * Grid formatter print.
 * @param heads table headers
 * @param dataset table data row set.
 */
func GridPrinf(title string, heads []string, dataset [][]interface{}) {
	printStr := "No result rows."
	if dataset != nil && len(dataset) > 0 {
		// Set go-tabulate writer.
		tabulate := gotabulate.Create(dataset)
		// Set the Empty String (optional)
		tabulate.SetEmptyString("None")
		// Set Align (Optional)
		tabulate.SetAlign("center")
		// Set Max Cell Size
		tabulate.SetMaxCellSize(16)
		// Set the Headers (optional)
		tabulate.SetHeaders(heads)
		printStr = tabulate.Render("grid")
	}

	// Print the result: grid, or simple.
	fmt.Printf("========== %s ==========\n%s\n", title, printStr)
}
