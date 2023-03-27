package autoconfig

import (
	"fmt"
	"time"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/timeutils"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/importer"
)

type ConfigCheckResults struct {
	Errors   []string
	Warnings []string
	Info     []string
}

func (c ConfigCheckResults) String() string {
	var s string

	if len(c.Errors) > 0 {
		s += "\tErrors:\n"
		for _, e := range c.Errors {
			s += fmt.Sprintf("\t\t%s\n", e)
		}
	}

	if len(c.Warnings) > 0 {
		s += "\tWarnings:\n"
		for _, e := range c.Warnings {
			s += fmt.Sprintf("\t\t%s\n", e)
		}
	}

	if len(c.Info) > 0 {
		s += "\tInfo:\n"
		for _, e := range c.Info {
			s += fmt.Sprintf("\t\t%s\n", e)
		}
	}

	return s
}

func ConfigCheck(appConfig *AppConfig) (ConfigCheckResults, error) {
	var funcName string = "autoconfig.ConfigCheck"
	var taskName string = "Performing Config Check"

	var start time.Time = time.Now()
	var took time.Duration
	var configCheckResults ConfigCheckResults

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("%v\n", taskName),
	)

	// Populate Platform Detail
	configCheckResults = PopulatePlatformDetail(appConfig)
	if len(configCheckResults.Errors) > 0 {
		return configCheckResults, nil
	}

	// Check the Config
	configCheckResults, err := checkConfig(appConfig)
	if err != nil {
		return ConfigCheckResults{}, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", taskName, took),
	)

	return configCheckResults, nil
}

func PopulatePlatformDetail(appConfig *AppConfig) ConfigCheckResults {
	var taskName string = "Populating Platform Detail"

	var configCheckResults ConfigCheckResults

	logger.Log(
		logger.LVL_DEBUG,
		taskName,
	)

	// Assign current config to local variables
	var userDBConfig engine.SEConfig = appConfig.ImporterConfig.DB
	var userImporterConfig importer.Config = appConfig.ImporterConfig

	if userDBConfig.Engine == nil {
		configCheckResults.Errors = append(configCheckResults.Errors, "The dbengine flag must be provided")
		return configCheckResults
	}

	// Set up platform details
	platformDetail := PlatformDetail{
		RequestedEngine: *userDBConfig.Engine,
	}

	// Configure and Get Database Info
	platformDetail, _ = GetDatabaseConfig(platformDetail, userImporterConfig)
	appConfig.PlatformDetail = platformDetail

	// Write database config
	switch *userDBConfig.Engine {
	case engine.EngineMySQL:
		config := GetMySQLConfig(userDBConfig)
		autoConfigMySQL := engine.DBConfig{
			Host:    &config.Host,
			Port:    &config.Port,
			User:    &config.User,
			Pass:    &config.Pass,
			Schema:  &config.Schema,
			Timeout: &config.Timeout,
		}
		appConfig.ImporterConfig.DB.DBConfig = autoConfigMySQL
		logger.Log(
			logger.LVL_DEBUG,
			fmt.Sprintf("Auto configured MySQL\n%v", autoConfigMySQL),
		)
	case engine.EnginePostgres:
		config := GetPgSQLConfig(userDBConfig)
		autoConfigPgSQL := engine.DBConfig{
			Host:    &config.Host,
			Port:    &config.Port,
			User:    &config.User,
			Pass:    &config.Pass,
			Schema:  &config.Schema,
			Timeout: &config.Timeout,
		}
		appConfig.ImporterConfig.DB.DBConfig = autoConfigPgSQL
		logger.Log(
			logger.LVL_DEBUG,
			fmt.Sprintf("Auto configured PgSQL\n%v", autoConfigPgSQL),
		)
	}

	return ConfigCheckResults{}
}

func GetDatabaseConfig(platformDetail PlatformDetail, config importer.Config) (PlatformDetail, engine.SEConfig) {
	logger.Log(
		logger.LVL_DEBUG,
		"Get Database Config",
	)

	platformDetail.MySQLDetails = GetMySQLDetails(config)

	platformDetail.PgSQLDetails = GetPgSQLDetails(config)

	platformDetail.SQLiteDetails = GetSQLiteDetails(config)

	return platformDetail, config.DB
}

func checkConfig(appConfig *AppConfig) (ConfigCheckResults, error) {
	var funcName string = "autoconfig.checkConfig"
	var results ConfigCheckResults

	// Assign current config to local variables
	var platformDetail PlatformDetail = appConfig.PlatformDetail
	var mySQLDetails MySQLDetails = appConfig.PlatformDetail.MySQLDetails
	var pgSQLDetails PgSQLDetails = appConfig.PlatformDetail.PgSQLDetails
	var sqLiteDetails SQLiteDetails = appConfig.PlatformDetail.SQLiteDetails
	var importerConfig importer.Config = appConfig.ImporterConfig

	// Check Connectivity
	switch platformDetail.RequestedEngine {
	case engine.EngineMySQL:

		if mySQLDetails.CanConnect {
			results.Info = append(results.Info, "Connected to MySQL OK")
		} else {
			results.Errors = append(results.Errors, "Connect to MySQL Failed")
		}

		if !mySQLDetails.ClientInstalled {
			results.Warnings = append(results.Warnings, "The MySQL client does not appear to be available on this system")
		}

	case engine.EnginePostgres:

		if pgSQLDetails.CanConnect {
			results.Info = append(results.Info, "Connected to PgSQL OK")
		} else {
			results.Errors = append(results.Errors, "Connect to PgSQL Failed")
		}

		if !pgSQLDetails.ClientInstalled {
			results.Warnings = append(results.Warnings, "The MySQL client does not appear to be available on this system")
		}

	case engine.EngineSQLite:

		if sqLiteDetails.CanConnect {
			results.Info = append(results.Info, "Connected to SQLite OK")
		} else {
			results.Errors = append(results.Errors, "Connect to SQLite Failed")
		}
	}

	//// Log Files

	// Check Log Folder Exists And Is Writeable
	// n.b. this is where the TimingsLogFile and ChecksumLogFile are stored
	if fileutils.FolderExists(appConfig.LogsFolder) {
		results.Info = append(results.Info, "LogsFolder Exists")
	} else {
		results.Errors = append(results.Errors, "LogsFolder Does Not Exist")
	}
	isWriteable, err := fileutils.FolderIsWriteable(appConfig.LogsFolder)
	if err != nil {
		return ConfigCheckResults{}, err
	}
	if isWriteable {
		results.Info = append(results.Info, "LogsFolder is Writable")
	} else {
		results.Errors = append(results.Errors, "LogsFolder is Not Writable")
	}

	// TimingsLogFile
	if fileutils.FileIsWriteable(appConfig.TimingsLogFile) {
		results.Info = append(results.Info, "TimingsLogFile is Writable")
	} else {
		results.Errors = append(results.Errors, "TimingsLogFile is Not Writable")
	}

	// ChecksumLogFile
	if fileutils.FileIsWriteable(appConfig.ChecksumLogFile) {
		results.Info = append(results.Info, "ChecksumLogFile is Writable")
	} else {
		results.Errors = append(results.Errors, "ChecksumLogFile is Not Writable")
	}

	//// Importer Config

	// Datafolder Exists
	if fileutils.FolderExists(importerConfig.DataFolder) {
		results.Info = append(results.Info, "Datafolder Exists")
	} else {
		results.Errors = append(results.Errors, "Datafolder Does Not Exist")
	}

	// Shapefiles Exist?
	var shapeFiles []string
	shapeFiles, err = fileutils.Find(importerConfig.DataFolder, ".shp")
	if err != nil {
		return ConfigCheckResults{}, fmt.Errorf("%v: %v", funcName, err.Error())
	}
	if len(shapeFiles) == 0 {
		results.Warnings = append(results.Warnings, "No shapefiles exist in the datafolder")
	}

	// If Usefiles is Selected
	if importerConfig.UseFiles {
		// If Not Skipping Inserts MySQL Client Must Be Available
		if !importerConfig.SkipInserts && !mySQLDetails.ClientInstalled {
			results.Errors = append(results.Errors, "UseFiles Option Will Fail If Not Skipping Inserts and MySQL Client Is Not Available")
		}
	}

	return results, nil
}
