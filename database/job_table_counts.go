//nolint:gci
package database

import (
	"fmt"
	"strings"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/types"

	"github.com/rockwell-uk/csync/waitgroup"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-nationalgrid"
	"github.com/rockwell-uk/go-progress/progress"
)

type TableCountsJob struct{}

func (j *TableCountsJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
	var tasks []*progress.Task
	for layerType := range types.MapLayers.Ordered() {
		for square := range nationalgrid.NationalGridSquares {
			tasks = append(tasks, &progress.Task{
				ID:        fmt.Sprintf("%v_%v", layerType, square),
				Magnitude: 1,
			})
		}
	}

	job := progress.SetupJob(jobName, tasks)

	return job, nil
}

func (j *TableCountsJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	var totalRows int
	var tableCounts = make(map[string]int)

	if s, ok := input.(engine.StorageEngine); ok {
		wg := waitgroup.New()

		// Do the work
		for _, layerType := range types.MapLayers.Ordered() {
			for square := range nationalgrid.NationalGridSquares {
				task, _ := job.GetTask(fmt.Sprintf("%v_%v", layerType, square))
				task.Start()

				var rows int

				tableName := strings.ToLower(square)
				fullTableName := fmt.Sprintf("%s.%s", layerType, tableName)
				countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.GetTableName(fmt.Sprintf("%v.%v", layerType, tableName)))
				logger.Log(
					logger.LVL_INTERNAL,
					fmt.Sprintf("countSQL %v %v\n", layerType, countSQL),
				)

				err := s.GetDB(layerType).QueryRow(countSQL).Scan(&rows)
				if err != nil {
					logger.Log(
						logger.LVL_INTERNAL,
						err.Error(),
					)
				}

				tableCounts[fullTableName] = rows

				totalRows += rows

				wg.Add(1)
				go func() {
					task.End()
					job.UpdateBar()
					wg.Done()
				}()
			}
		}

		wg.Wait()

		return TableCountsResult{
			TotalRows:   totalRows,
			TableCounts: tableCounts,
		}, nil
	}

	return struct{}{}, nil
}
