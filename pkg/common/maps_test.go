package common

import (
	"fmt"
	"testing"
)

func TestToOrderedKeys(t *testing.T) {
	fmt.Printf("Testing %s starting ...\n", "TestToOrderedKeys")
	mymap := make(map[string]string)
	mymap["a3"] = "Jack"
	mymap["a1"] = "Washington"
	mymap["a2"] = "Marry"
	mymap["b1"] = "Saxon"
	mymap["c1"] = "Michael Jackson"
	mymap["d1"] = "Da Vinci"
	fmt.Println("---------- Ordered foreach map ------------")
	keys, err := ToOrderedKeys(mymap)
	if err != nil {
		panic("Error to orderedKeys")
	}
	for _, key := range keys {
		fmt.Printf("%s => %s\n", key, mymap[key])
	}
}
