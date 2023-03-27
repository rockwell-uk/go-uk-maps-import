package mysql

import (
	"fmt"

	"github.com/rockwell-uk/csync/waitgroup"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
)

func bulkAction(job *progress.Job, input interface{}, sqlCommandTpl string) (interface{}, error) {
	if e, ok := input.(MySQL); ok {
		var wg *waitgroup.WaitGroup = waitgroup.New()

		// Do the work
		for layerType, t := range job.Tasks {
			t.Start()
			wg.Add(1)

			go func(j *progress.Job, task *progress.Task, lt string) {
				rawSQL := fmt.Sprintf(sqlCommandTpl, lt)

				logger.Log(
					logger.LVL_DEBUG,
					fmt.Sprintf("%+v\n", rawSQL),
				)

				e.GetDB(lt).MustExec(rawSQL)
				task.End()
				j.UpdateBar()
				wg.Done()
			}(job, t, layerType)
		}

		wg.Wait()

		return struct{}{}, nil
	}

	return struct{}{}, nil
}
