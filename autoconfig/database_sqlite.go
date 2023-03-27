package autoconfig

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-sqlbench/benchmark"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/sqlite"
	"go-uk-maps-import/importer"
)

var (
	spatialiteDriverName string = "spatialite"
	defaultDriverName    string = "sqlite3"
)

type SQLiteDetails struct {
	DriverInstalled     bool
	SpatialiteInstalled bool
	FolderWriteable     bool
	CanConnect          bool
	ConnectionError     *string
	BenchmarkResult     benchmark.BenchmarkResult
}

func (d SQLiteDetails) String() string {
	return fmt.Sprintf("\t\t\t"+"DriverInstalled: %v"+"\n"+
		"\t\t\t"+"SpatialiteInstalled: %v"+"\n"+
		"\t\t\t"+"FolderWriteable: %v"+"\n"+
		"\t\t\t"+"CanConnect: %v"+"\n"+
		"\t\t\t"+"BenchmarkResult: \n%v",
		d.DriverInstalled,
		d.SpatialiteInstalled,
		d.FolderWriteable,
		d.CanConnect,
		d.BenchmarkResult,
	)
}

func GetSQLiteDetails(config importer.Config) SQLiteDetails {
	logger.Log(
		logger.LVL_DEBUG,
		"Get SQLite Details",
	)

	cfg := GetSQLiteConfig(config.DB)

	var driverInstalled bool = checkSQLiteInstalled()
	var spatialiteInstalled bool = checkSpatialiteInstalled()
	var folderWriteable bool = checkSQLiteFolderWriteable()
	var canConnect bool = checkSQLiteConnectivity(defaultDriverName)
	var connectionError *string

	if driverInstalled && spatialiteInstalled && folderWriteable && canConnect {
		se := &sqlite.SQLite{
			Config: cfg,
		}

		config.DB.StorageEngine = se

		var benchDBFile string = se.GetDatabasePath("bench")
		db, err := connectSQLite(spatialiteDriverName, benchDBFile)
		if err != nil {
			errString := err.Error()
			connectionError = &errString
		}
		db.Close()
		os.Remove(benchDBFile)
	}

	return SQLiteDetails{
		DriverInstalled:     driverInstalled,
		SpatialiteInstalled: spatialiteInstalled,
		FolderWriteable:     folderWriteable,
		CanConnect:          canConnect,
		ConnectionError:     connectionError,
	}
}

func GetSQLiteConfig(userConfig engine.SEConfig) sqlite.SQLiteConfig {
	return sqlite.SQLiteConfig{}
}

func connectSQLite(driverName, dbFile string) (*sqlx.DB, error) {
	var funcName string = "autoconfig.connectSQLite"

	db, err := sqlx.Connect(driverName, dbFile)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return db, nil
}

func checkSQLiteConnectivity(driverName string) bool {
	db, _ := connectSQLite(driverName, ":memory:")
	if db == nil {
		return false
	}
	db.Close()

	return true
}

//nolint:nonamedreturns
func checkSQLiteInstalled() (isInstalled bool) {
	defer func() {
		if r := recover(); r != nil {
			isInstalled = true
		}
	}()

	sql.Register(defaultDriverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return nil
		},
	})

	return isInstalled
}

func checkSpatialiteInstalled() bool {
	var isInstalled bool

	sql.Register(spatialiteDriverName, &sqlite3.SQLiteDriver{
		Extensions: []string{"mod_spatialite"},
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			isInstalled = true
			return nil
		},
	})

	// Need to connect to trigger connect hook
	_, _ = connectSQLite(spatialiteDriverName, ":memory:")

	return isInstalled
}

func checkSQLiteFolderWriteable() bool {
	isWriteable, err := fileutils.FolderIsWriteable(sqlite.SQLiteStorageFolder)
	if err != nil {
		return false
	}

	return isWriteable
}
