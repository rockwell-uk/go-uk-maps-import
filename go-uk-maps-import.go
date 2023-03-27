package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/timeutils"

	"go-uk-maps-import/autoconfig"
	"go-uk-maps-import/database"
	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/filelogger"
	"go-uk-maps-import/importer"
	"go-uk-maps-import/osdata"
)

var (
	start time.Time = time.Now()

	datafolder  string = "./resources/mapdata-source-files/shp/"
	auto        bool   = false
	download    bool   = false
	cleardown   bool   = false
	concurrent  bool   = false
	unlimited   bool   = false
	skipinserts bool   = false
	usefiles    bool   = false
	lowmemory   bool   = false
	countsonly  bool   = false
	dryrun      bool   = false

	dbengine  *string
	dbhost    *string
	dbport    *string
	dbuser    *string
	dbpass    *string
	dbschema  *string
	dbtimeout *int

	dbe string
	dbh string
	dbt string
	dbu string
	dbp string
	dbs string
	dbo int
)

const (
	logFolder   string = "logs"
	timingsLog  string = "timings.log"
	checksumLog string = "checksum.log"
)

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func main() {
	var funcName string = "main.main"

	// Define and parse flags
	var v, vv, vvv bool
	flag.BoolVar(&v, "v", false, "APP level log verbosity override")
	flag.BoolVar(&vv, "vv", false, "DEBUG level log verbosity override")
	flag.BoolVar(&vvv, "vvv", false, "INTERNAL level log verbosity override")

	// Auto Configure
	flag.BoolVar(&auto, "auto", auto, "run platform benchmarks, and automatically configure the application?")

	// Shapefiles Data Folder
	flag.StringVar(&datafolder, "datafolder", datafolder, "the folder to scan for shapefiles to import")

	// Download osdata source files?
	flag.BoolVar(&download, "download", download, "download the osdata source files?")

	// Cleardown the database?
	flag.BoolVar(&cleardown, "cleardown", cleardown, "clear down the database?")

	// Use intermediate SQL files?
	flag.BoolVar(&usefiles, "usefiles", usefiles, "use intermediate SQL files?")

	// Refrain from loading shapefiles into memory?
	flag.BoolVar(&lowmemory, "lowmemory", lowmemory, "do not read the shapefiles into memory?")

	// Process all shapefiles at once?
	flag.BoolVar(&concurrent, "concurrent", concurrent, "process multiple shapefiles at once?")

	// Process all shapefiles at once?
	flag.BoolVar(&unlimited, "unlimited", unlimited, "process all the shapefiles at once?")

	// Skip processing the .sql files?
	flag.BoolVar(&skipinserts, "skipinserts", skipinserts, "we skip importing the .sql files?")

	// Just count the database rows and exit?
	flag.BoolVar(&countsonly, "countsonly", countsonly, "just count the database rows and exit?")

	// Exit before running the import?
	flag.BoolVar(&dryrun, "dryrun", dryrun, "exit the app before running the import?")

	// Database
	flag.StringVar(&dbe, "dbengine", dbe, "the database engine mysql/postgres/sqlite")
	flag.StringVar(&dbh, "dbhost", dbh, "the database host")
	flag.StringVar(&dbt, "dbport", dbt, "the database port")
	flag.StringVar(&dbu, "dbuser", dbu, "the database username")
	flag.StringVar(&dbp, "dbpass", dbp, "the database password")
	flag.StringVar(&dbs, "dbschema", dbs, "the base database schema")
	flag.IntVar(&dbo, "dbtimeout", dbo, "the database dbtimeout")

	flag.Parse()

	if isFlagPassed("dbengine") {
		dbengine = &dbe
	}
	if isFlagPassed("dbhost") {
		dbhost = &dbh
	}
	if isFlagPassed("dbport") {
		dbport = &dbt
	}
	if isFlagPassed("dbuser") {
		dbuser = &dbu
	}
	if isFlagPassed("dbpass") {
		dbpass = &dbp
	}
	if isFlagPassed("dbschema") {
		dbschema = &dbs
	}
	if isFlagPassed("dbtimeout") {
		dbtimeout = &dbo
	}

	// Start the main logger
	var vbs logger.LogLvl
	switch {
	case vvv:
		vbs = logger.LVL_INTERNAL
	case vv:
		vbs = logger.LVL_DEBUG
	case v:
		vbs = logger.LVL_APP
	}
	startLoggers(vbs)

	// Log start time
	logAppStart()

	// Log files
	checksumLog := getLogFileName(checksumLog)
	timingsLog := getLogFileName(timingsLog)

	// Clear logs
	err := clearLogs()
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			fmt.Sprintf("%v: Error clearing logs: %v", funcName, err.Error()),
		)
		bailOut(1)
	}

	// Open logfiles
	checksumLogFile, err := fileutils.GetFile(checksumLog)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			fmt.Sprintf("%v: Error opening log file: %v", funcName, err.Error()),
		)
		bailOut(1)
	}
	defer checksumLogFile.Close()

	timingsLogFile, err := fileutils.GetFile(timingsLog)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			fmt.Sprintf("%v: Error opening log file: %v", funcName, err.Error()),
		)
		bailOut(1)
	}
	defer timingsLogFile.Close()

	// Download Ordnance Survey Data
	if download {
		err := osdata.DownloadVectorMapDistrict()
		if err != nil {
			logger.Log(
				logger.LVL_FATAL,
				fmt.Sprintf("%v: Error downloading osdata: %v", funcName, err.Error()),
			)
			bailOut(1)
		}
	}

	shapefilesToImport, err := importer.GetAllShapefiles(datafolder)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			err.Error(),
		)
		bailOut(1)
	}

	// Base App Config
	var appConfig *autoconfig.AppConfig = &autoconfig.AppConfig{
		SystemDetails:   autoconfig.GetSystemDetails(),
		LogsFolder:      logFolder,
		ChecksumLogFile: checksumLog,
		TimingsLogFile:  timingsLog,
		ImporterConfig: importer.Config{
			DataFolder:    datafolder,
			ShapeFiles:    shapefilesToImport,
			NumShapeFiles: len(shapefilesToImport),
			Download:      download,
			Concurrent:    concurrent,
			Unlimited:     unlimited,
			SkipInserts:   skipinserts,
			UseFiles:      usefiles,
			LowMemory:     lowmemory,
			TimingsLog:    timingsLogFile,
			ChecksumLog:   checksumLogFile,
			DB: engine.SEConfig{
				Engine: dbengine,
				DBConfig: engine.DBConfig{
					Host:    dbhost,
					Port:    dbport,
					User:    dbuser,
					Pass:    dbpass,
					Schema:  dbschema,
					Timeout: dbtimeout,
				},
				ClearDown:  cleardown,
				CountsOnly: countsonly,
			},
		},
		DryRun: dryrun,
	}

	// Config Check
	configCheckResults, err := autoconfig.ConfigCheck(appConfig)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			fmt.Sprintf("%v: Error running config check: %v", funcName, err.Error()),
		)
		bailOut(1)
	}
	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("Config Check Results:\n%v", configCheckResults),
	)

	// Bail out if Errors
	if len(configCheckResults.Errors) > 0 {
		bailOut(1)
	}

	// Autoconfig
	if auto {
		err := autoconfig.AutoConfigure(appConfig)
		if err != nil {
			logger.Log(
				logger.LVL_FATAL,
				fmt.Sprintf("%v: Error running autoconfig: %v", funcName, err.Error()),
			)
			bailOut(1)
		}
	}

	// Log app config
	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("App Config:"+
			"\n\t"+"Log Verbosity: %v"+
			"\n\t"+"Timings Log: %v"+
			"\n\t"+"Checksum Log: %v"+
			"\n\t"+"Importer Config:"+"\n"+"%v"+
			"\n\t"+"Database Config:"+"\n"+"%v"+
			"\n\t"+"System:"+"\n"+"%v"+
			"\n\t"+"Platform:"+"\n"+"%v",
			vbs,
			appConfig.TimingsLogFile,
			appConfig.ChecksumLogFile,
			appConfig.ImporterConfig,
			appConfig.ImporterConfig.DB,
			appConfig.SystemDetails,
			appConfig.PlatformDetail,
		),
	)

	// Start any dependencies
	startApp(appConfig)

	// Action
	switch {
	case appConfig.ImporterConfig.DB.CountsOnly:
		tableCounts, err := database.GetTableCounts(appConfig.ImporterConfig.DB.StorageEngine)
		if err != nil {
			logger.Log(
				logger.LVL_FATAL,
				fmt.Sprintf("%v: Error running import: %v", funcName, err.Error()),
			)
			bailOut(1)
		}

		logger.Log(
			logger.LVL_APP,
			fmt.Sprintf("%v rows in database\n", tableCounts.TotalRows),
		)

		logger.Log(
			logger.LVL_DEBUG,
			fmt.Sprintf("TableCounts: %v\n", tableCounts.TableCounts),
		)

	case !appConfig.DryRun:
		err := importer.Run(start, appConfig.ImporterConfig)
		if err != nil {
			logger.Log(
				logger.LVL_FATAL,
				fmt.Sprintf("%v: Error running import: %v", funcName, err.Error()),
			)
			bailOut(1)
		}
	}

	// Stop any dependencies and cleanup
	stopApp(appConfig)
}

func startLoggers(vbs logger.LogLvl) {
	// Start main logger
	logger.Start(vbs)

	// Start file logger
	filelogger.Start()
}

func startApp(appConfig *autoconfig.AppConfig) {
	var funcName string = "main.startApp"

	// Startup database connections
	err := engine.Startup(appConfig.DryRun, &appConfig.ImporterConfig.DB)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			fmt.Sprintf("%v: Error starting database engine: %v", funcName, err.Error()),
		)
		bailOut(1)
	}
}

func stopLoggers() {
	// Stop main logger
	logger.Stop()

	// Stop file logger
	filelogger.Stop()
}

func stopApp(appConfig *autoconfig.AppConfig) {
	var funcName string = "main.stopApp"

	// Shutdown database connections
	err := engine.Shutdown(appConfig.DryRun, appConfig.ImporterConfig.DB)
	if err != nil {
		logger.Log(
			logger.LVL_FATAL,
			fmt.Sprintf("%v: Error shutting down database engine %v", funcName, err.Error()),
		)
		bailOut(1)
	}

	// Log how long things took
	logRuntime(appConfig.ImporterConfig.TimingsLog)

	// Bye
	bailOut(0)
}

func logAppStart() {
	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("App started %s", timeutils.FormatTime(start)),
	)
}

func logRuntime(logFile io.Writer) {
	var end string = timeutils.FormatTime(time.Now())
	var took time.Duration = timeutils.Took(start)

	logAppStart()

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("App ended %s", end),
	)

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("App ran for %v", took),
	)

	filelogger.Log(
		filelogger.LogLine{
			File: logFile,
			Line: fmt.Sprintf("import took %v", took),
		},
	)
}

func bailOut(exitCode int) {
	stopLoggers()

	// Give time for logging to happen ¯\_(ツ)_/¯
	time.Sleep(time.Millisecond * 5)

	// Exit
	os.Exit(exitCode)
}

func clearLogs() error {
	var funcName string = "main.clearLogs"

	err := fileutils.MkDir(logFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = fileutils.EmptyFolder(logFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return nil
}

func getLogFileName(name string) string {
	return fmt.Sprintf("%v/%v", logFolder, name)
}
