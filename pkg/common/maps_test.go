package common

import (
	"fmt"
	"testing"
)

func TestToOrderedKeys(t *testing.T) {
	fmt.Printf("Testing %s starting ...\n", "TestToOrderedKeys")
	mymap := make(map[string]int)
	mymap["a1"] = 11
	mymap["a2"] = 22
	mymap["b1"] = 33
	mymap["b1"] = 44
	fmt.Println(mymap)
}
