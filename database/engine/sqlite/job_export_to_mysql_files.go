package sqlite

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/rockwell-uk/csync/mutex"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-nationalgrid"
	"github.com/rockwell-uk/go-progress/progress"

	"go-uk-maps-import/database/types"
	"go-uk-maps-import/osdata"
	"go-uk-maps-import/sqlwriter"
)

// ref: https://groups.google.com/g/spatialite-users/c/U2pxp3bwVnY

var sqlFilesWritten = make(map[string]bool)

type ExportToMySQLFilesJob struct{}

func (j *ExportToMySQLFilesJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
	var tasks = make([]*progress.Task, len(types.MapLayers))
	for i, layerType := range types.MapLayers.Ordered() {
		tasks[i] = &progress.Task{
			ID:        layerType,
			Magnitude: 1,
		}
	}

	job := progress.SetupJob(jobName, tasks)

	return job, nil
}

func (j *ExportToMySQLFilesJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if e, ok := input.(SQLite); ok {
		// Do the work
		for layerType, task := range job.Tasks {
			task.Start()

			db := e.GetDB(layerType)
			layerTypeFields := types.MapLayers[layerType]

			logger.Log(
				logger.LVL_DEBUG,
				fmt.Sprintf("Writing files for %v\n", layerType),
			)

			for square := range nationalgrid.NationalGridSquares {
				tableName := strings.ToLower(square)

				// Only do the queries if we're going to log the result
				if logger.Vbs == logger.LVL_INTERNAL {
					countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %v", tableName)
					var numRows int
					row := db.QueryRow(countQuery)
					err := row.Scan(&numRows)
					if err != nil {
						return struct{}{}, err
					}
					logger.Log(
						logger.LVL_INTERNAL,
						fmt.Sprintf("%v rows for square %v\n", numRows, square),
					)
				}

				query := fmt.Sprintf("SELECT %v, AsBinary(ogc_geom) AS ogc_geom FROM %v", strings.Join(layerTypeFields, ","), tableName)
				rows, err := db.Queryx(query)
				if err != nil {
					return struct{}{}, err
				}
				defer rows.Close()

				var sqlFilePath string

				for rows.Next() {
					mutex.Lock()

					result := make(map[string]interface{})
					err = rows.MapScan(result)
					if err != nil {
						return struct{}{}, err
					}

					s := result["GRIDREF"]

					fieldValues := ""
					fieldNames := ""

					for _, fieldName := range layerTypeFields {
						fieldNames += fmt.Sprintf("%v, ", fieldName)
						value := result[fieldName]

						switch v := value.(type) {
						case string:
							// Fix invalid UTF8 strings
							v = osdata.InvalidUTF8Fix(v)

							// Fix for FEATCODE in "some" shapefiles
							if fieldName == "FEATCODE" {
								asInt := osdata.FeatcodeFix(v)
								fieldValues += fmt.Sprintf("%v", asInt)
							} else {
								fieldValues += fmt.Sprintf(`"%v", `, v)
							}

						default:
							fieldValues += fmt.Sprintf("%v, ", v)
						}
					}

					fullTableName := fmt.Sprintf("%s.%s", layerType, tableName)

					sqlFileName := fmt.Sprintf("%s%s", tableName, fmt.Sprintf("%02d", s))
					sqlFilePath = fmt.Sprintf("%s.%s", layerType, sqlFileName)

					if _, exists := sqlFilesWritten[sqlFilePath]; !exists {
						sqlwriter.Write(
							sqlwriter.SQLLine{
								DBName: layerType,
								Table:  sqlFileName,
								Line:   fmt.Sprintf(`REPLACE INTO %s (%vogc_geom) VALUES `, fullTableName, fieldNames),
							},
						)

						if _, exists := sqlFilesWritten[fullTableName]; !exists {
							sqlFilesWritten[fullTableName] = true
						}
						sqlFilesWritten[sqlFilePath] = true
					}

					if ogc_geom, ok := result["ogc_geom"].([]byte); ok {
						sqlwriter.Write(
							sqlwriter.SQLLine{
								DBName: layerType,
								Table:  sqlFileName,
								Line:   fmt.Sprintf(`(%vST_GeomFromWKB(X'%v')),`, fieldValues, hex.EncodeToString(ogc_geom)),
							},
						)
					}

					mutex.Unlock()
				}
			}

			task.End()
			job.UpdateBar()
		}
	}

	return struct{}{}, nil
}
