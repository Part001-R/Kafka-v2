package kafkaproducer

import (
	"fmt"

	"github.com/google/uuid"
)

// Generate random uuid. Returne keys and error.
func KeysGenerate(size int) (keys []string, err error) {

	for i := 0; i < size; i++ {

		val, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("Fault generate uuid: <%w>", err)
		}

		keys = append(keys, val.String())
	}

	return keys, nil
}
