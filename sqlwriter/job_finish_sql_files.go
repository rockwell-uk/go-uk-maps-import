package sqlwriter

import (
	"fmt"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
)

type FinishSQLFilesJob struct{}

func (j *FinishSQLFilesJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
	if sqlFiles, ok := input.([]string); ok {
		var tasks []*progress.Task
		for _, sqlFile := range sqlFiles {
			tasks = append(tasks, &progress.Task{
				ID:        sqlFile,
				Magnitude: 1,
			})
		}

		job := progress.SetupJob(jobName, tasks)

		return job, nil
	}

	return nil, fmt.Errorf("expected []string got %T", input)
}

func (j *FinishSQLFilesJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	// Do the work
	for _, task := range job.Tasks {
		task.Start()

		logger.Log(
			logger.LVL_INTERNAL,
			task.ID,
		)

		err := removeLastComma(task.ID)
		if err != nil {
			return struct{}{}, err
		}

		task.End()
		job.UpdateBar()
	}

	return struct{}{}, nil
}
