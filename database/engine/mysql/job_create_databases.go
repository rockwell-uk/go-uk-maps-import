package mysql

import (
	"github.com/rockwell-uk/go-progress/progress"

	"go-uk-maps-import/database/types"
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
	sqlAction := "CREATE DATABASE IF NOT EXISTS `%s`"

	return bulkAction(job, input, sqlAction)
}
