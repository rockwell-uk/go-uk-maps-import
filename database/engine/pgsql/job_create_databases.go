package pgsql

import (
	"go-uk-maps-import/database/types"

	"github.com/rockwell-uk/go-progress/progress"
)

type CreateDatabasesJob struct{}

func (j *CreateDatabasesJob) Setup(jobName string, input interface{}) (*progress.Job, error) {

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

func (j *CreateDatabasesJob) Run(job *progress.Job, input interface{}) (interface{}, error) {

	sqlAction := "CREATE SCHEMA IF NOT EXISTS %v"

	return bulkAction(job, input, sqlAction)
}
