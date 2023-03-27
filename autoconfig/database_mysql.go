package autoconfig

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-sqlbench/benchmark"
	"github.com/rockwell-uk/go-utils/osutils"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/mysql"
	"go-uk-maps-import/importer"
)

var defaultMySQLPort = "3306"

type MySQLDetails struct {
	DriverInstalled bool
	ClientInstalled bool
	CanConnect      bool
	ConnectionError *string
	BenchmarkResult benchmark.BenchmarkResult
}

func (d MySQLDetails) String() string {
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

func GetMySQLDetails(config importer.Config) MySQLDetails {
	logger.Log(
		logger.LVL_DEBUG,
		"Get MySQL Details",
	)

	cfg := GetMySQLConfig(config.DB)

	var driverInstalled bool = checkMySQLDriverInstalled()
	var clientInstalled bool = checkMySQLClientInstalled()
	var canConnect bool = checkMySQLConnectivity(cfg)
	var connectionError *string

	if driverInstalled && clientInstalled && canConnect {
		config.DB.StorageEngine = &mysql.MySQL{
			Config: cfg,
		}

		db, err := connectMySQL(cfg)
		if err != nil {
			errString := err.Error()
			connectionError = &errString
		}
		db.Close()
	}

	return MySQLDetails{
		DriverInstalled: driverInstalled,
		ClientInstalled: clientInstalled,
		CanConnect:      canConnect,
		ConnectionError: connectionError,
	}
}

func GetMySQLConfig(userConfig engine.SEConfig) mysql.MySQLConfig {
	switch *userConfig.Engine {
	case engine.EngineMySQL:
		return mysql.MySQLConfig{
			Host:    getHost(userConfig.DBConfig),
			Port:    getPort(userConfig.DBConfig, defaultMySQLPort),
			User:    getUser(userConfig.DBConfig),
			Pass:    getPass(userConfig.DBConfig),
			Schema:  getSchema(userConfig.DBConfig),
			Timeout: getTimeout(userConfig.DBConfig),
		}
	default:
		return mysql.MySQLConfig{
			Host:    defaultHost,
			Port:    defaultMySQLPort,
			User:    defaultUser,
			Pass:    defaultPass,
			Schema:  defaultSchema,
			Timeout: defaultTimeout,
		}
	}
}

func connectMySQL(dbConfig mysql.MySQLConfig) (*sqlx.DB, error) {
	var funcName string = "autoconfig.connectMySQL"

	db, err := sqlx.Open(engine.EngineMySQL, dbConfig.DSN())
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return db, nil
}

func checkMySQLConnectivity(dbConfig mysql.MySQLConfig) bool {
	db, _ := connectMySQL(dbConfig)
	if db == nil {
		return false
	}
	db.Close()

	return true
}

func checkMySQLDriverInstalled() bool {
	// @TODO figure out if we need / can do this
	return true
}

func checkMySQLClientInstalled() bool {
	return osutils.CommandExists("mysql")
}
