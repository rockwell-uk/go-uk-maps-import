package pgsql

import (
	"github.com/rockwell-uk/go-progress/progress"

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
	sqlAction := "DROP SCHEMA IF EXISTS %s CASCADE"

	return bulkAction(job, input, sqlAction)
}
