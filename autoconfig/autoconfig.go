package autoconfig

import (
	"fmt"
	"time"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/timeutils"

	"go-uk-maps-import/importer"
)

type AppConfig struct {
	SystemDetails   SystemDetails
	PlatformDetail  PlatformDetail
	ImporterConfig  importer.Config
	LogsFolder      string
	TimingsLogFile  string
	ChecksumLogFile string
	DryRun          bool
}

type PlatformDetail struct {
	RequestedEngine       string
	MySQLDetails          MySQLDetails
	PgSQLDetails          PgSQLDetails
	SQLiteDetails         SQLiteDetails
	SufficientCPUCores    *bool
	SufficientFileHandles *bool
	DBIsSlow              *bool
	FsIsSlow              *bool
}

func (p PlatformDetail) String() string {
	var sufficientCPUCores string = "not checked"
	if p.SufficientCPUCores != nil {
		sufficientCPUCores = fmt.Sprintf("%v", *p.SufficientCPUCores)
	}

	var sufficientFileHandles string = "not checked"
	if p.SufficientFileHandles != nil {
		sufficientFileHandles = fmt.Sprintf("%v", *p.SufficientFileHandles)
	}

	var dbIsSlow string = "not checked"
	if p.SufficientCPUCores != nil {
		dbIsSlow = fmt.Sprintf("%v", *p.DBIsSlow)
	}

	var fsIsSlow string = "not checked"
	if p.FsIsSlow != nil {
		fsIsSlow = fmt.Sprintf("%v", *p.FsIsSlow)
	}

	return fmt.Sprintf("\t\t"+"RequestedEngine: %v"+"\n"+
		"\t\t"+"MySQLDetails: \n%v"+"\n"+
		"\t\t"+"PgSQLDetails: \n%v"+"\n"+
		"\t\t"+"SQLiteDetails: \n%v"+"\n"+
		"\t\t"+"SufficientCPUCores: %v"+"\n"+
		"\t\t"+"SufficientFileHandles: %v"+"\n"+
		"\t\t"+"DBIsSlow: %v"+"\n"+
		"\t\t"+"FsIsSlow: %v",
		p.RequestedEngine,
		p.MySQLDetails,
		p.PgSQLDetails,
		p.SQLiteDetails,
		sufficientCPUCores,
		sufficientFileHandles,
		dbIsSlow,
		fsIsSlow,
	)
}

func AutoConfigure(appConfig *AppConfig) error {
	var funcName string = "autoconfig.AutoConfigure"
	var taskName string = "App Auto Config"

	var start time.Time = time.Now()
	var took time.Duration

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("%v\n", taskName),
	)

	// Assign current config to local variables
	var userImporterConfig importer.Config = appConfig.ImporterConfig
	var systemDetails SystemDetails = appConfig.SystemDetails
	var platformDetail *PlatformDetail = &appConfig.PlatformDetail

	// Attempt to set necessary system parameters
	err := SetupSystem(userImporterConfig, &systemDetails)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	// Disk benchmark
	doDiskBench(&systemDetails)

	// Storage benchmark
	doStorageBench(userImporterConfig, platformDetail)

	// Check / ReWrite the Config
	var importerConfig importer.Config = GetImporterConfig(platformDetail, systemDetails, userImporterConfig)

	// Save AutoConfigured Values
	appConfig.ImporterConfig = importerConfig
	appConfig.SystemDetails = systemDetails

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", taskName, took),
	)

	return nil
}
