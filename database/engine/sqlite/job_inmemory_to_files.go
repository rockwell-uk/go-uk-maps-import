package sqlite

import (
	"fmt"
	"os"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/database/types"
)

type InMemoryToFilesJob struct{}

func (j *InMemoryToFilesJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
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

func (j *InMemoryToFilesJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if e, ok := input.(SQLite); ok {
		i := 0
		// Write the in memory databases to files
		for layerType, dbConn := range driverConns {
			task, _ := job.GetTask(layerType)
			task.Start()

			dbFilePath := fmt.Sprintf("%v/%v.db", SQLiteStorageFolder, layerType)

			if fileutils.FileExists(dbFilePath) {
				logger.Log(
					logger.LVL_DEBUG,
					fmt.Sprintf("Deleting file %v\n", dbFilePath),
				)
				os.Remove(dbFilePath)
			}

			logger.Log(
				logger.LVL_DEBUG,
				fmt.Sprintf("Writing %v\n", dbFilePath),
			)

			err := doBackup(e.dbs[layerType], dbConn, layerType, dbFilePath, false)
			if err != nil {
				logger.Log(
					logger.LVL_FATAL,
					err.Error(),
				)
				return struct{}{}, err
			}

			task.End()
			job.UpdateBar()
			i++
		}
	}

	return struct{}{}, nil
}
