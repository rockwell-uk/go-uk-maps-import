package sqlite

import (
	"fmt"
	"os"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/database/types"
)

type ClearDownJob struct{}

func (j *ClearDownJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
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

func (j *ClearDownJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if _, ok := input.(SQLite); ok {
		// Do the work
		for layerType, task := range job.Tasks {
			task.Start()

			dbFilePath := fmt.Sprintf("%v/%v.db", SQLiteStorageFolder, layerType)

			if !fileutils.FileExists(dbFilePath) {
				logger.Log(
					logger.LVL_DEBUG,
					fmt.Sprintf("Database file %v does not exist\n", dbFilePath),
				)
			} else {
				logger.Log(
					logger.LVL_DEBUG,
					fmt.Sprintf("Deleting file %v\n", dbFilePath),
				)
				os.Remove(dbFilePath)
			}

			journalPath := fmt.Sprintf("%v/%v.db-journal", SQLiteStorageFolder, layerType)
			os.Remove(journalPath)

			task.End()
			job.UpdateBar()
		}

		return struct{}{}, nil
	}

	return struct{}{}, nil
}
