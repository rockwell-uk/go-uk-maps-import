package autoconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/pbnjay/memory"
	"github.com/rockwell-uk/go-diskbench/diskbench"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-sqlbench/benchmark"
	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/osutils"
	"github.com/rockwell-uk/go-utils/timeutils"

	"go-uk-maps-import/database"
	"go-uk-maps-import/database/engine/mysql"
	"go-uk-maps-import/database/engine/pgsql"
	"go-uk-maps-import/database/engine/sqlite"
	"go-uk-maps-import/importer"
	"go-uk-maps-import/sqlwriter"
)

const (
	mysqlBenchNumRows  = 25000
	pgsqlBenchNumRows  = 25000
	sqliteBenchNumRows = 10000
)

type SystemDetails struct {
	OS        string
	Arch      string
	NumCPU    int
	TotalMem  uint64
	FreeMem   uint64
	DiskSpeed diskbench.DiskBenchResult
	ULimit    int
}

func (c SystemDetails) String() string {
	return fmt.Sprintf("\t\t"+"OS: %v"+"\n"+
		"\t\t"+"Arch: %v"+"\n"+
		"\t\t"+"NumCPU: %v"+"\n"+
		"\t\t"+"TotalMem: %v"+"\n"+
		"\t\t"+"FreeMem: %v"+"\n"+
		"\t\t"+"DiskSpeed: \n%v"+"\n"+
		"\t\t"+"ULimit: %v",
		c.OS,
		c.Arch,
		c.NumCPU,
		fmt.Sprintf("%.2f%v", fileutils.ByteSizeConvert(int64(c.TotalMem), "gb"), "gb"),
		fmt.Sprintf("%.2f%v", fileutils.ByteSizeConvert(int64(c.FreeMem), "gb"), "gb"),
		c.DiskSpeed,
		c.ULimit,
	)
}

func SetupSystem(importerConfig importer.Config, systemDetails *SystemDetails) error {
	var funcName string = "autoconfig.SetupSystem"

	var requiredFileHandlesForUnlimited uint64 = uint64(importerConfig.NumShapeFiles * fileHandlesPerShapefile)

	var rLimit syscall.Rlimit

	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	if rLimit.Cur > requiredFileHandlesForUnlimited {
		return nil
	}
	if rLimit.Max < requiredFileHandlesForUnlimited {
		return nil
	}
	if rLimit.Max < requiredFileHandlesForUnlimited {
		rLimit.Cur = rLimit.Max
	} else {
		rLimit.Cur = requiredFileHandlesForUnlimited
	}

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	ulimit, _ := osutils.GetULimit()
	systemDetails.ULimit = ulimit

	return nil
}

func GetSystemDetails() SystemDetails {
	ulimit, _ := osutils.GetULimit()

	sytemDetails := SystemDetails{
		OS:       osutils.GetOS(),
		Arch:     osutils.GetArch(),
		NumCPU:   runtime.NumCPU(),
		TotalMem: memory.TotalMemory(),
		FreeMem:  memory.FreeMemory(),
		ULimit:   ulimit,
	}

	return sytemDetails
}

func doDiskBench(systemDetails *SystemDetails) {
	var taskName string = "Benchmarking Disk"

	var start time.Time = time.Now()
	var took time.Duration
	var absPath string

	absPath, _ = filepath.Abs(sqlwriter.Folder)
	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("%v [path: %v]\n", taskName, absPath),
	)

	benchResult, _ := diskbench.BenchDisk(
		diskbench.DiskBench{
			Folder:  sqlwriter.Folder,
			Seconds: 5,
		},
	)
	systemDetails.DiskSpeed = benchResult

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", taskName, took),
	)
}

func doStorageBench(config importer.Config, platformDetails *PlatformDetail) {
	var benchmarkResult benchmark.BenchmarkResult
	var mySQLDetails MySQLDetails = platformDetails.MySQLDetails
	var pgSQLDetails PgSQLDetails = platformDetails.PgSQLDetails
	var sqLiteDetails SQLiteDetails = platformDetails.SQLiteDetails

	// MySQL
	if mySQLDetails.DriverInstalled && mySQLDetails.ClientInstalled && mySQLDetails.CanConnect {
		cfg := GetMySQLConfig(config.DB)

		config.DB.StorageEngine = &mysql.MySQL{
			Config: cfg,
		}

		db, err := connectMySQL(cfg)
		if err != nil {
			benchmarkResult = benchmark.BenchmarkResult{
				Err: err,
			}
			platformDetails.MySQLDetails.BenchmarkResult = benchmarkResult
			return
		}

		platformDetails.MySQLDetails.BenchmarkResult = database.Benchmark(db, "MySQL", mysql.MySQLTableParams, mysqlBenchNumRows)
		db.Close()
	}

	// PgSQL
	if pgSQLDetails.DriverInstalled && pgSQLDetails.ClientInstalled && pgSQLDetails.CanConnect {
		cfg := GetPgSQLConfig(config.DB)

		config.DB.StorageEngine = &pgsql.PgSQL{
			Config: cfg,
		}

		db, err := connectPgSQL(cfg)
		if err != nil {
			benchmarkResult = benchmark.BenchmarkResult{
				Err: err,
			}
			platformDetails.PgSQLDetails.BenchmarkResult = benchmarkResult
			return
		}

		platformDetails.PgSQLDetails.BenchmarkResult = database.Benchmark(db, "PgSQL", pgsql.PgSQLTableParams, pgsqlBenchNumRows)
		db.Close()
	}

	// SQLite
	if sqLiteDetails.DriverInstalled && sqLiteDetails.SpatialiteInstalled && sqLiteDetails.FolderWriteable && sqLiteDetails.CanConnect {
		cfg := sqlite.SQLiteConfig{}

		se := &sqlite.SQLite{
			Config: cfg,
		}

		config.DB.StorageEngine = se

		var benchDBFile string = se.GetDatabasePath("bench")
		db, err := connectSQLite(spatialiteDriverName, benchDBFile)
		if err != nil {
			benchmarkResult = benchmark.BenchmarkResult{
				Err: err,
			}
		} else {
			benchmarkResult = database.Benchmark(db, "SQLite", sqlite.SQLiteTableParams, sqliteBenchNumRows)
			db.Close()
			os.Remove(benchDBFile)
		}

		platformDetails.SQLiteDetails.BenchmarkResult = benchmarkResult
	}
}
