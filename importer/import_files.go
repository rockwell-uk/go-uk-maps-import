package importer

import (
	"encoding/hex"
	"fmt"
	"sort"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rockwell-uk/csync/mutex"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-nationalgrid"
	"github.com/rockwell-uk/go-shpconvert/shpconvert"
	"github.com/twpayne/go-geos"

	"go-uk-maps-import/sqlwriter"
)

var sqlFilesWritten = make(map[string]bool)

func importToFile(ctx *geos.Context, r importAction, dbName string) importResult {
	var rowsGenerated = make(map[string]int)

	b, err := shpconvert.ShpToWKB(r.shape)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			err.Error(),
		)
	}

	shapeGeom, err := ctx.NewGeomFromWKB(b)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			err.Error(),
		)
	}

	fieldNames, fieldValues := getFieldNamesAndValues(r.insert)

	for square, subSquares := range nationalgrid.GetSubSquares(shapeGeom.Bounds()) {
		fullTableName := fmt.Sprintf("%s.%s", dbName, square)

		mutex.Lock()

		for _, s := range subSquares {
			sqlFileName := fmt.Sprintf("%s%s", square, fmt.Sprintf("%02d", s))
			sqlFilePath := fmt.Sprintf("%s.%s", dbName, sqlFileName)

			if _, exists := sqlFilesWritten[sqlFilePath]; !exists {
				sqlwriter.Write(
					sqlwriter.SQLLine{
						DBName: dbName,
						Table:  sqlFileName,
						Line:   fmt.Sprintf(`REPLACE INTO %s (GRIDREF, %vogc_geom) VALUES `, fullTableName, fieldNames),
					},
				)

				if _, exists := sqlFilesWritten[fullTableName]; !exists {
					rowsGenerated[square] = 0
					sqlFilesWritten[fullTableName] = true
				}

				sqlFilesWritten[sqlFilePath] = true
			}

			sqlwriter.Write(
				sqlwriter.SQLLine{
					DBName: dbName,
					Table:  sqlFileName,
					Line:   fmt.Sprintf(`(%v, %vST_GeomFromWKB(X'%v')),`, s, fieldValues, hex.EncodeToString(b)),
				},
			)

			rowsGenerated[square]++
		}

		mutex.Unlock()
	}

	return importResult{
		rowsGenerated: rowsGenerated,
	}
}

func getFieldNamesAndValues(i insert) (string, string) {
	var fieldNames, fieldValues string

	// need consistent order
	mapped := make(map[string]interface{})
	for name, value := range i {
		mapped[name] = value
	}

	keys := make([]string, 0, len(mapped))
	for k := range mapped {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		fieldNames += fmt.Sprintf("%v, ", key)
		fieldValues += fmt.Sprintf("%#v, ", mapped[key])
	}

	return fieldNames, fieldValues
}
