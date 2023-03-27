package sqlite

import (
	"fmt"

	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/database/types"
)

// ref: https://groups.google.com/g/spatialite-users/c/U2pxp3bwVnY

var targetFolder string = "sql"

func (e SQLite) ExportToMySQLFiles() error {
	var funcName string = "sqlite.ExportToMySQLFiles"
	var jobName string = "Exporting SQLite databases to MySQL format files"

	var magnitude int = len(types.MapLayers)

	// Pre flight
	err := fileutils.MkDir(targetFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = fileutils.EmptyFolder(targetFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	// ExportToMySQLFiles Job
	var job progress.ProgressJob = &ExportToMySQLFilesJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e SQLite) ExportToSQLiteFiles() error {
	var funcName string = "mysql.ExportToSQLiteFiles"
	var jobName string = "Exporting SQLite databases to SQLite format files"

	var magnitude int = len(types.MapLayers)

	// Pre flight
	err := fileutils.MkDir(targetFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = fileutils.EmptyFolder(targetFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	// ExportToMySQLFiles Job
	var job progress.ProgressJob = &ExportToSQLiteFilesJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}
