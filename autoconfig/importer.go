package autoconfig

import (
	"fmt"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/importer"
)

const (
	mysqlSpeedTidemark  = 10 // seconds for 25000 queries
	sqliteSpeedTidemark = 12 // seconds for 10000 queries

	fsBenchTidemark = 70 // num files written

	lowMemoryTidemark = 2 // gb

	minCPUForConcurrency = 2

	fileHandlesPerShapefile = 2
	minCPUForUnlimited      = 8
)

/*
	DataFolder    string // ignore / dont change, should we test its readable?
	Download      bool   // ignore / dont change
	SkipInserts   bool   // ignore / dont change

	Concurrent    bool   // configure based on number of CPU's
	Unlimited     bool   // configure based on number of CPU's && available file handles
	UseFiles      bool   // if database is slow but the filesystem is fast, use this option
	LowMemory     bool   // configure based on available memory
*/

func GetImporterConfig(platformDetail *PlatformDetail, systemDetails SystemDetails, importerConfig importer.Config) importer.Config {
	// AutoConfigure Importer Options
	var importerAutoConfig importer.Config = importerConfig

	importerAutoConfig.Concurrent = concurrencyFlagEnabled(systemDetails)
	importerAutoConfig.Unlimited = unlimitedFlagEnabled(platformDetail, systemDetails, importerConfig)
	importerAutoConfig.UseFiles = useFilesFlagEnabled(platformDetail, systemDetails)
	importerAutoConfig.LowMemory = lowMemoryFlagEnabled(systemDetails)

	return importerAutoConfig
}

func concurrencyFlagEnabled(systemDetails SystemDetails) bool {
	availableCPUs := systemDetails.NumCPU
	var concurrencyFlagEnabled bool = availableCPUs > minCPUForConcurrency

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("availableCPUs %v\n", availableCPUs),
	)
	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("minCPUForConcurrency %v\n", minCPUForConcurrency),
	)
	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("concurrencyFlagEnabled %v\n", concurrencyFlagEnabled),
	)

	return concurrencyFlagEnabled
}

func unlimitedFlagEnabled(platformDetail *PlatformDetail, systemDetails SystemDetails, importerConfig importer.Config) bool {
	var sufficientCPUCores bool = (systemDetails.NumCPU >= minCPUForUnlimited)
	var sufficientFileHandles bool = (systemDetails.ULimit >= (importerConfig.NumShapeFiles * fileHandlesPerShapefile))

	platformDetail.SufficientCPUCores = &sufficientCPUCores
	platformDetail.SufficientFileHandles = &sufficientFileHandles

	var unlimitedFlagEnabled bool = sufficientCPUCores && sufficientFileHandles

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("sufficientCPUCores %v\n", sufficientCPUCores),
	)

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("sufficientFileHandles %v\n", sufficientFileHandles),
	)

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("unlimitedFlagEnabled %v\n", unlimitedFlagEnabled),
	)

	return unlimitedFlagEnabled
}

func useFilesFlagEnabled(platformDetail *PlatformDetail, systemDetails SystemDetails) bool {
	var dbIsSlow bool = databaseIsSlow(*platformDetail)
	var fsIsSlow bool = filesystemIsSlow(systemDetails)

	platformDetail.DBIsSlow = &dbIsSlow
	platformDetail.FsIsSlow = &fsIsSlow

	var useFilesFlagEnabled bool = dbIsSlow && !fsIsSlow && platformDetail.MySQLDetails.ClientInstalled

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("dbIsSlow %v\n", dbIsSlow),
	)

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("fsIsSlow %v\n", fsIsSlow),
	)

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("useFilesFlagEnabled %v\n", useFilesFlagEnabled),
	)

	return useFilesFlagEnabled
}

func lowMemoryFlagEnabled(systemDetails SystemDetails) bool {
	var freeMemoryGb float64 = fileutils.ByteSizeConvert(int64(systemDetails.FreeMem), "gb")
	var lowMemory bool = freeMemoryGb < lowMemoryTidemark

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("freeMemoryGb %v\n", freeMemoryGb),
	)
	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("lowMemoryTidemark %v\n", lowMemoryTidemark),
	)
	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("lowMemory %v\n", lowMemory),
	)

	return lowMemory
}

func databaseIsSlow(platformDetail PlatformDetail) bool {
	var dbIsSlow bool

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("requestedEngine %v\n", platformDetail.RequestedEngine),
	)

	switch platformDetail.RequestedEngine {
	case engine.EngineMySQL:

		dbIsSlow = platformDetail.MySQLDetails.BenchmarkResult.Duration < mysqlSpeedTidemark
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("benchmarkResultDuration %v\n", platformDetail.MySQLDetails.BenchmarkResult.Duration),
		)
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("mysqlSpeedTidemark %v\n", mysqlSpeedTidemark),
		)
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("dbIsSlow %v\n", dbIsSlow),
		)

		return dbIsSlow

	case engine.EngineSQLite:

		dbIsSlow = platformDetail.SQLiteDetails.BenchmarkResult.Duration < sqliteSpeedTidemark
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("benchmarkResultDuration %v\n", platformDetail.SQLiteDetails.BenchmarkResult.Duration),
		)
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("sqliteSpeedTidemark %v\n", sqliteSpeedTidemark),
		)
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("dbIsSlow %v\n", dbIsSlow),
		)

		return dbIsSlow
	}

	return dbIsSlow
}

func filesystemIsSlow(systemDetails SystemDetails) bool {
	var filesystemIsSlow bool = systemDetails.DiskSpeed.Writes < fsBenchTidemark

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("filesystemIsSlow %v\n", filesystemIsSlow),
	)

	return filesystemIsSlow
}
