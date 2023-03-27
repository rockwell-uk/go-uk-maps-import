package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-nationalgrid"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"

	"go-uk-maps-import/database/types"
)

const (
	SQLiteTableParams string = ""
)

var (
	SQLiteStorageFolder = "db"
	driverConns         = map[string]*sqlite3.SQLiteConn{}
	driverName          string
)

type SQLiteConfig struct {
	Host    string
	Port    string
	User    string
	Pass    string
	Schema  string
	Timeout int
}

func (c SQLiteConfig) DSN() string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?timeout=%vs",
		c.User,
		c.Pass,
		c.Host,
		c.Port,
		c.Schema,
		c.Timeout,
	)
}

type SQLite struct {
	Config SQLiteConfig
	dbs    map[string]*sqlx.DB
}

func (e *SQLite) Connect() error {
	var funcName string = "sqlite.Connect"
	var jobName string = "Connecting SQLite Databases"

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("%v\n", jobName),
	)

	driverName = fmt.Sprintf("sqlite3_with_spatialite_%v", time.Now().UnixNano())

	var driverConn *sqlite3.SQLiteConn
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		Extensions: []string{"mod_spatialite"},
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			driverConn = conn
			return nil
		},
	})

	e.dbs = make(map[string]*sqlx.DB)

	err := fileutils.MkDir(SQLiteStorageFolder)
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	for _, layerType := range types.MapLayers.Ordered() {
		db, err := sqlx.Connect(driverName, ":memory:")
		if err != nil {
			return fmt.Errorf("%v %v", funcName, err.Error())
		}

		// See "Important settings" section.
		db.SetMaxOpenConns(1)

		driverConns[layerType] = driverConn
		e.dbs[layerType] = db
	}

	logger.Log(
		logger.LVL_DEBUG,
		"Connected\n",
	)

	return nil
}

func (e SQLite) Cleardown() error {
	var funcName string = "sqlite.Cleardown"
	var jobName string = "Cleardown SQLite Database"

	var magnitude int = len(types.MapLayers)

	// Cleardown Job
	var job progress.ProgressJob = &ClearDownJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e SQLite) Prepare() error {
	var funcName string = "sqlite.Prepare"

	err := e.CreateTables()
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	return nil
}

func (e SQLite) CreateTables() error {
	var funcName string = "sqlite.CreateTables"
	var jobName string = "Creating SQLite Tables"

	var magnitude int = len(types.MapLayers) * len(nationalgrid.NationalGridSquares)

	// Create Tables Job
	var job progress.ProgressJob = &CreateTablesJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e SQLite) GetDB(layerType string) *sqlx.DB {
	return e.dbs[layerType]
}

func (e SQLite) InMemoryToFiles() error {
	var funcName string = "sqlite.InMemoryToFiles"
	var jobName string = "Writing SQLite in memory databases to files"

	var magnitude int = len(driverConns)

	// In Memory To Files Job
	var job progress.ProgressJob = &InMemoryToFilesJob{}

	if len(driverConns) == 0 {
		return fmt.Errorf("%v: no driver connections were found", funcName)
	}

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("Driver connections %v\n", driverConns),
	)

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e SQLite) SwitchToFiles() error {
	var funcName string = "sqlite.SwitchToFiles"

	e.dbs = make(map[string]*sqlx.DB)

	err := fileutils.MkDir(SQLiteStorageFolder)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	for layerType := range types.MapLayers {
		dbFilePath := e.GetDatabasePath(layerType)
		exists := fileutils.FileExists(dbFilePath)
		if !exists {
			return fmt.Errorf("%v: db file does not exist %v", funcName, dbFilePath)
		}

		db, err := sqlx.Connect(driverName, dbFilePath)
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}
		e.dbs[layerType] = db
	}

	return nil
}

func (e SQLite) Stop() error {
	for layerType := range types.MapLayers {
		// Delete the driver conn
		delete(driverConns, layerType)

		// Close the database
		e.dbs[layerType].Close()
	}

	return nil
}

func (e SQLite) GetTableName(batchInsertsKey string) string {
	s := strings.Split(batchInsertsKey, ".")
	return s[1]
}

func (e SQLite) GetDatabasePath(dbname string) string {
	return fmt.Sprintf("%v/%v.db", SQLiteStorageFolder, dbname)
}

func (e SQLite) GetTableSQL(fullTableName, tableParams string, fields []string) (string, error) {
	tableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", fullTableName)

	for _, f := range fields {
		if _, ok := types.FieldTypes[f]; !ok {
			return "", fmt.Errorf("unknown field type (%v) %v", fullTableName, f)
		}

		tableSQL += fmt.Sprintf("`%s` %s,", f, types.FieldTypes[f])
	}

	tableSQL += fmt.Sprintf("`ogc_geom` geometry DEFAULT NULL, PRIMARY KEY (`ID`, `GRIDREF`))%v;", tableParams)

	return tableSQL, nil
}
