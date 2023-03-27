package sqlwriter

import (
	"io"

	"github.com/rockwell-uk/go-progress/progress"
)

func CalculateChecksums(sqlFiles []string, logFile io.Writer) error {
	var funcName string = "sqlwriter.CalculateChecksums"
	var jobName string = "Calculating checksums"

	var magnitude int = len(sqlFiles)

	// Calculate Checksums Job
	var job progress.ProgressJob = &CalculateChecksumsJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, sqlFiles, logFile)
}
