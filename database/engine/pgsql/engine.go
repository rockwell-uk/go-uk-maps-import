//nolint:gci
package pgsql

import (
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/jmoiron/sqlx"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"

	"go-uk-maps-import/database/types"
)

const (
	PgSQLTableParams string = ""
)

type PgSQLConfig struct {
	Host    string
	Port    string
	User    string
	Pass    string
	Schema  string
	Timeout int
}

func (c PgSQLConfig) String() string {
	return fmt.Sprintf("\t\t\t"+"Host: %v"+"\n"+
		"\t\t\t"+"Port: %v"+"\n"+
		"\t\t\t"+"User: %v"+"\n"+
		"\t\t\t"+"Pass: %v"+"\n"+
		"\t\t\t"+"Schema: %v"+"\n"+
		"\t\t\t"+"Timeout: %v",
		c.Host,
		c.Port,
		c.User,
		"****",
		c.Schema,
		c.Timeout,
	)
}

func (c PgSQLConfig) DSN() string {
	return fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v connect_timeout=%v sslmode=disable",
		c.User,
		c.Pass,
		c.Host,
		c.Port,
		c.Schema,
		c.Timeout,
	)
}

type PgSQL struct {
	Config PgSQLConfig
	DB     *sqlx.DB
}

func (e *PgSQL) Connect() error {
	var funcName string = "pgsql.Connect"
	var taskName string = "Connecting to PgSQL database"

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("%v\n", taskName),
	)

	// Configure DSN
	dsn := e.Config.DSN()

	// Connect
	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// Attach
	e.DB = db

	logger.Log(
		logger.LVL_DEBUG,
		"Connected\n",
	)

	return nil
}

func (e PgSQL) Cleardown() error {
	var funcName string = "pgsql.Cleardown"
	var jobName string = "Cleardown PgSQL Database"

	var magnitude int = len(types.MapLayers)

	// Cleardown Job
	var job progress.ProgressJob = &ClearDownJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e PgSQL) Prepare() error {
	var funcName string = "pgsql.Prepare"

	err := e.CreateDatabases()
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	err = e.CreateTables()
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	return nil
}

func (e PgSQL) CreateDatabases() error {
	var funcName string = "pgsql.CreateDatabases"
	var jobName string = "Creating PgSQL Databases"

	var magnitude int = len(types.MapLayers)

	// Create Databases Job
	var job progress.ProgressJob = &CreateDatabasesJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e PgSQL) CreateTables() error {
	var funcName string = "pgsql.CreateTables"
	var jobName string = "Creating PgSQL Tables"

	var magnitude int = len(types.MapLayers)

	// Create Tables Job
	var job progress.ProgressJob = &CreateTablesJob{}

	return progress.RunJob(jobName, funcName, job, magnitude, struct{}{}, e)
}

func (e PgSQL) GetDB(layerType string) *sqlx.DB {
	return e.DB
}

func (e PgSQL) Stop() error {
	e.DB.Close()
	return nil
}

func (e PgSQL) GetTableName(batchInsertsKey string) string {
	return batchInsertsKey
}

func (e PgSQL) GetTableSQL(fullTableName, tableParams string, fields []string) (string, error) {
	tableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", fullTableName)

	for _, f := range fields {
		if _, ok := types.FieldTypes[f]; !ok {
			return "", fmt.Errorf("unknown field type (%v) %v", fullTableName, f)
		}

		tableSQL += fmt.Sprintf("%s %s,", f, types.FieldTypes[f])
	}

	tableSQL += fmt.Sprintf("ogc_geom geometry DEFAULT NULL, UNIQUE (ID, GRIDREF))%v;", tableParams)

	return tableSQL, nil
}
