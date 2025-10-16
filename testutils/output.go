package testutils

import (
	"encoding/json"
	"fmt"
)

// Function only for tests and visual identify structures.
func OutputIndent(v interface{}) {
	blob, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(blob))
}
