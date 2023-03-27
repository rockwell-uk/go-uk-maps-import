package engine

import (
	"fmt"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"
)

type DoInsertsJob struct{}

func (j *DoInsertsJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
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

func (j *DoInsertsJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if dbConfig, ok := input.(DBConfig); ok {
		// Do the work
		for _, task := range job.Tasks {
			task.Start()

			sqlFile := task.ID

			fileSizeMb, err := fileutils.FileSize(sqlFile, "mb")
			if err != nil {
				return struct{}{}, err
			}

			logger.Log(
				logger.LVL_DEBUG,
				fmt.Sprintf("Processing %v [%v]\n", sqlFile, fileSizeMb),
			)

			stdout, stderr, err := RunSQLFile(dbConfig, sqlFile)
			if err != nil {
				return struct{}{}, fmt.Errorf("%v: stdout [%v], stderr [%v]", err.Error(), stdout, stderr)
			}

			task.End()
			job.UpdateBar()
		}
	}

	return struct{}{}, nil
}
