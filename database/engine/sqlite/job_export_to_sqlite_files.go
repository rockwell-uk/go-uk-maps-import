package sqlite

import (
	"bufio"
	"bytes"
	"fmt"
	"os"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/schollz/sqlite3dump"

	"go-uk-maps-import/database/types"
)

// ref: https://groups.google.com/g/spatialite-users/c/U2pxp3bwVnY

type ExportToSQLiteFilesJob struct{}

func (j *ExportToSQLiteFilesJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
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

func (j *ExportToSQLiteFilesJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if _, ok := input.(SQLite); ok {
		// Do the work
		for layerType, task := range job.Tasks {
			task.Start()

			dbFilePath := fmt.Sprintf("%v/%v.db", SQLiteStorageFolder, layerType)

			var b bytes.Buffer
			out := bufio.NewWriter(&b)
			options := []sqlite3dump.Option{
				sqlite3dump.WithTransaction(false),
				// sqlite3dump.WithDropIfExists(true),
				sqlite3dump.WithMigration(),
			}
			err := sqlite3dump.Dump(dbFilePath, out, options...)
			if err != nil {
				return struct{}{}, err
			}

			sqlFilePath := fmt.Sprintf("%v/%v.sql", targetFolder, layerType)

			logger.Log(
				logger.LVL_DEBUG,
				fmt.Sprintf("Writing %v\n", sqlFilePath),
			)
			out.Flush()
			err = os.WriteFile(sqlFilePath, b.Bytes(), 0600)
			if err != nil {
				return struct{}{}, err
			}

			task.End()
			job.UpdateBar()
		}
	}

	return struct{}{}, nil
}
