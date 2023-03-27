package autoconfig

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-sqlbench/benchmark"
	"github.com/rockwell-uk/go-utils/osutils"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/pgsql"
	"go-uk-maps-import/importer"
)

var defaultPgSQLPort = "5432"

type PgSQLDetails struct {
	DriverInstalled bool
	ClientInstalled bool
	CanConnect      bool
	ConnectionError *string
	BenchmarkResult benchmark.BenchmarkResult
}

func (d PgSQLDetails) String() string {
	return fmt.Sprintf("\t\t\t"+"DriverInstalled: %v"+"\n"+
		"\t\t\t"+"ClientInstalled: %v"+"\n"+
		"\t\t\t"+"CanConnect: %v"+"\n"+
		"\t\t\t"+"BenchmarkResult: \n%v",
		d.DriverInstalled,
		d.ClientInstalled,
		d.CanConnect,
		d.BenchmarkResult,
	)
}

func GetPgSQLDetails(config importer.Config) PgSQLDetails {
	logger.Log(
		logger.LVL_DEBUG,
		"Get PgSQL Details",
	)

	cfg := GetPgSQLConfig(config.DB)

	var driverInstalled bool = checkPgSQLDriverInstalled()
	var clientInstalled bool = checkPgSQLClientInstalled()
	var canConnect bool = checkPgSQLConnectivity(cfg)
	var connectionError *string

	if driverInstalled && clientInstalled && canConnect {
		config.DB.StorageEngine = &pgsql.PgSQL{
			Config: cfg,
		}

		db, err := connectPgSQL(cfg)
		if err != nil {
			errString := err.Error()
			connectionError = &errString
		}
		db.Close()
	}

	return PgSQLDetails{
		DriverInstalled: driverInstalled,
		ClientInstalled: clientInstalled,
		CanConnect:      canConnect,
		ConnectionError: connectionError,
	}
}

func GetPgSQLConfig(userConfig engine.SEConfig) pgsql.PgSQLConfig {
	switch *userConfig.Engine {
	case engine.EnginePostgres:
		return pgsql.PgSQLConfig{
			Host:    getHost(userConfig.DBConfig),
			Port:    getPort(userConfig.DBConfig, defaultPgSQLPort),
			User:    getUser(userConfig.DBConfig),
			Pass:    getPass(userConfig.DBConfig),
			Schema:  getSchema(userConfig.DBConfig),
			Timeout: getTimeout(userConfig.DBConfig),
		}
	default:
		return pgsql.PgSQLConfig{
			Host:    defaultHost,
			Port:    defaultPgSQLPort,
			User:    defaultUser,
			Pass:    defaultPass,
			Schema:  defaultSchema,
			Timeout: defaultTimeout,
		}
	}
}

func connectPgSQL(dbConfig pgsql.PgSQLConfig) (*sqlx.DB, error) {
	var funcName string = "autoconfig.connectPgSQL"

	db, err := sqlx.Open("postgres", dbConfig.DSN())
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return db, nil
}

func checkPgSQLConnectivity(dbConfig pgsql.PgSQLConfig) bool {
	db, _ := connectPgSQL(dbConfig)
	if db == nil {
		return false
	}
	db.Close()

	return true
}

func checkPgSQLDriverInstalled() bool {
	// @TODO figure out if we need / can do this
	return true
}

func checkPgSQLClientInstalled() bool {
	return osutils.CommandExists("psql")
}
