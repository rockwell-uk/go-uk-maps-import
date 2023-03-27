package importer

import (
	"fmt"
)

type batchInsert []insert

func (b batchInsert) String() string {
	var r string

	for layerType, inserts := range b {
		r += fmt.Sprintf("\n%+v:\n", layerType)

		for field, insert := range inserts {
			r += fmt.Sprintf("\t%+v: %+v\n", field, insert)
		}
	}

	return r
}

type insert map[string]interface{}

func (i insert) String() string {
	var r string

	for k, insert := range i {
		r += fmt.Sprintf("%v: %v\n", k, insert)
	}

	return r
}
