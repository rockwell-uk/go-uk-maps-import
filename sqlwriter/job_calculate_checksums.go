package sqlwriter

import (
	"fmt"
	"io"

	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/filelogger"
)

type CalculateChecksumsJob struct{}

func (j *CalculateChecksumsJob) Setup(jobName string, input interface{}) (*progress.Job, error) {
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

func (j *CalculateChecksumsJob) Run(job *progress.Job, input interface{}) (interface{}, error) {
	if logFile, ok := input.(io.Writer); ok {
		// Do the work
		for sqlFile, task := range job.Tasks {
			task.Start()

			fileHash, err := fileutils.FileHash(sqlFile)
			if err != nil {
				return struct{}{}, err
			}

			filelogger.Log(
				filelogger.LogLine{
					File: logFile,
					Line: fmt.Sprintf("%v [%v]", sqlFile, fileHash),
				},
			)

			task.End()
			job.UpdateBar()
		}

		return struct{}{}, nil
	}

	return nil, fmt.Errorf("expected []string got %T", input)
}
