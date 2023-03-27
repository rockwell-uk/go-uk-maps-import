package sqlwriter

import (
	"github.com/rockwell-uk/go-progress/progress"
)

func FinishSQLFiles(sqlFiles []string) error {
	var funcName string = "sqlwriter.FinishSQLFiles"
	var jobName string = "Finishing SQL Files"

	var magnitude int = len(sqlFiles)

	// Finish SQL Files Job
	var job progress.ProgressJob = &FinishSQLFilesJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, sqlFiles, struct{}{})
}
