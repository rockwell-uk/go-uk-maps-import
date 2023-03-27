package importer

import (
	"fmt"
	"strings"

	"github.com/rockwell-uk/datastore"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-nationalgrid"
	"github.com/rockwell-uk/go-shpconvert/shpconvert"
	"github.com/twpayne/go-geos"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/pgsql"
)

func importDirect(ctx *geos.Context, r importAction, dbName, sfShortName string) importResult {
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

	for square, subSquares := range nationalgrid.GetSubSquares(shapeGeom.Bounds()) {
		batchInsertsKey := fmt.Sprintf("%v.%v", dbName, square)
		batchInserts := loadBatchInserts(sfShortName)

		if _, exists := rowsGenerated[square]; !exists {
			rowsGenerated[square] = 0
		}

		for _, gridRef := range subSquares {
			fvClone := insert{}
			for key, value := range r.insert {
				fvClone[key] = value
			}

			fvClone["GRIDREF"] = gridRef
			fvClone["ogc_geom"] = b

			batchInserts[batchInsertsKey] = append(batchInserts[batchInsertsKey], fvClone)

			rowsGenerated[square]++
		}

		saveBatchInserts(sfShortName, batchInserts)
	}

	return importResult{
		rowsGenerated: rowsGenerated,
	}
}

func getDBName(batchInsertsKey string) string {
	s := strings.Split(batchInsertsKey, ".")

	return s[0]
}

func loadBatchInserts(batchInsertsKey string) map[string]batchInsert {
	var empty = make(map[string]batchInsert)

	batchInserts, err := datastore.Get(batchInsertsKey)
	if err != nil {
		saveBatchInserts(batchInsertsKey, empty)
		return empty
	}

	if inserts, ok := batchInserts.(map[string]batchInsert); ok {
		return inserts
	}

	return empty
}

//nolint:errcheck
func saveBatchInserts(batchInsertsKey string, batchInserts map[string]batchInsert) {
	datastore.Put(batchInsertsKey, batchInserts)
}

func runInserts(se engine.StorageEngine, batchInsertsKey string) {
	var funcName string = "importer.runInserts"

	batchInserts := loadBatchInserts(batchInsertsKey)

	for key, inserts := range batchInserts {
		var dbName = getDBName(key)
		tableName := se.GetTableName(key)

		fieldsMap, exists := dbFieldsMap[dbName]
		if !exists {
			err := fmt.Errorf("dbFieldsMap does not exits %v", dbName)
			panic(err)
		}

		fieldNames := fieldsMap.fieldNames
		placeHolders := fieldsMap.placeHolders

		var leadLine string
		var query string

		switch se.(type) {
		case *pgsql.PgSQL:
			leadLine = fmt.Sprintf(`INSERT INTO %s (%v) VALUES `, tableName, fieldNames)
			query = fmt.Sprintf("%+v (%+v)", leadLine, placeHolders)
			query = fmt.Sprintf("%v ON CONFLICT (ID, GRIDREF) DO NOTHING", query)
		default:
			leadLine = fmt.Sprintf(`REPLACE INTO %s (%v) VALUES `, tableName, fieldNames)
			query = fmt.Sprintf("%+v (%+v)", leadLine, placeHolders)
		}

		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("[%v.%v] upserting %v records\n", dbName, tableName, len(inserts)),
		)

		_, err := se.GetDB(dbName).NamedExec(query, inserts)
		if err != nil {
			var msg string = fmt.Sprintf("[%v.%v]:\n%v\n\n %+v\n\n (%+v)\n\n %+v\n\n", dbName, tableName, err.Error(), query, inserts, dbFieldsMap)
			logger.Log(
				logger.LVL_FATAL,
				err.Error(),
			)

			showTablesSQL := "SHOW TABLES"
			tables := []string{}
			err = se.GetDB(dbName).Select(&tables, showTablesSQL)
			if err != nil {
				logger.Log(
					logger.LVL_FATAL,
					err.Error(),
				)
			}

			db := se.GetDB(dbName)
			panic(fmt.Errorf("%v: %v, tables %v [%+v]", funcName, msg, tables, db))
		}

		delete(batchInserts, key)
	}

	saveBatchInserts(batchInsertsKey, batchInserts)
}
