package sqlite

import (
	"fmt"
	"strings"

	"github.com/rockwell-uk/csync/mutex"
	"github.com/rockwell-uk/csync/waitgroup"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-nationalgrid"
	"github.com/rockwell-uk/go-progress/progress"

	"go-uk-maps-import/database/types"
)

type CreateTablesJob struct{}

func (j *CreateTablesJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
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

func (j *CreateTablesJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if e, ok := input.(SQLite); ok {
		var wg *waitgroup.WaitGroup = waitgroup.New()

		for _, layerType := range types.MapLayers.Ordered() {
			fields := types.MapLayers[layerType]

			for square := range nationalgrid.NationalGridSquares {
				wg.Add(1)

				c := make(chan error)
				go func(sq, lt string, f []string) {
					task, _ := job.GetTask(fmt.Sprintf("%v_%v", lt, sq))
					task.Start()

					tableName := strings.ToLower(sq)
					tableSQL, err := e.GetTableSQL(tableName, SQLiteTableParams, f)
					if err != nil {
						c <- err
					}

					logger.Log(
						logger.LVL_DEBUG,
						fmt.Sprintf("[%v] %+v\n", layerType, tableSQL),
					)

					mutex.Lock()
					e.GetDB(lt).MustExec(tableSQL)

					countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %v", tableName)
					var numRows int
					row := e.GetDB(lt).QueryRow(countQuery)
					err = row.Scan(&numRows)
					if err != nil {
						panic(err)
					}
					logger.Log(
						logger.LVL_DEBUG,
						fmt.Sprintf("%v rows for table [%v.%v]\n", numRows, lt, tableName),
					)
					mutex.Unlock()

					task.End()
					job.UpdateBar()

					wg.Done()

					c <- nil
				}(square, layerType, fields)

				err := <-c
				if err != nil {
					return struct{}{}, err
				}
			}
		}

		wg.Wait()

		return struct{}{}, nil
	}

	return struct{}{}, nil
}
