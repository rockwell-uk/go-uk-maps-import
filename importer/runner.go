package importer

import (
	"fmt"
	"time"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/timeutils"

	"go-uk-maps-import/database"
	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/mysql"
	"go-uk-maps-import/database/engine/sqlite"
	"go-uk-maps-import/rates"
	"go-uk-maps-import/sqlwriter"
)

func Run(start time.Time, config Config) error {
	var funcName string = "runner.Run"
	var importStart time.Time = time.Now()

	if config.UseFiles {
		// Start SQL Writer
		sqlwriter.Start()
	}

	// Do the import
	rateInfo, err := doImport(config)
	if err != nil {
		return fmt.Errorf("%v %v", funcName, err.Error())
	}

	switch se := config.DB.StorageEngine.(type) {
	case *mysql.MySQL:

		if config.UseFiles {
			// Stop SQL Writer
			sqlwriter.Stop()

			// SQL files that were generated
			sqlFiles, err := database.GetGeneratedSQLFiles(sqlwriter.Folder)
			if err != nil {
				return fmt.Errorf("%v %v", funcName, err.Error())
			}

			// Finish off the SQL files
			err = sqlwriter.FinishSQLFiles(sqlFiles)
			if err != nil {
				return fmt.Errorf("%v %v", funcName, err.Error())
			}

			// Checksums
			err = sqlwriter.CalculateChecksums(sqlFiles, config.ChecksumLog)
			if err != nil {
				return fmt.Errorf("%v %v", funcName, err.Error())
			}

			// Database Inserts
			if !config.SkipInserts {
				err = engine.DoInserts(config.DB.DBConfig, sqlFiles)
				if err != nil {
					return fmt.Errorf("%v %v", funcName, err.Error())
				}
			}
		}

	case *sqlite.SQLite:

		// Write sqlite in-memory databases to files
		err = se.InMemoryToFiles()
		if err != nil {
			return fmt.Errorf("%v %v", funcName, err.Error())
		}

		// Why? ¯\_(ツ)_/¯
		if config.UseFiles {
			// Export the SQLite databases to .sql files
			err = se.ExportToMySQLFiles()
			if err != nil {
				return fmt.Errorf("%v %v", funcName, err.Error())
			}

			// Stop SQL Writer
			sqlwriter.Stop()

			// Connect and prepare MySQL
			e := &mysql.MySQL{
				Config: mysql.MySQLConfig{
					Host:    *config.DB.DBConfig.Host,
					Port:    *config.DB.DBConfig.Port,
					User:    *config.DB.DBConfig.User,
					Pass:    *config.DB.DBConfig.Pass,
					Schema:  *config.DB.DBConfig.Schema,
					Timeout: *config.DB.DBConfig.Timeout,
				},
			}
			err := e.Connect()
			if err != nil {
				return fmt.Errorf("%v %v", funcName, err.Error())
			}
			err = e.Prepare()
			if err != nil {
				return fmt.Errorf("%v %v", funcName, err.Error())
			}

			if !config.SkipInserts {
				// SQL files that were generated
				sqlFiles, err := database.GetGeneratedSQLFiles(sqlwriter.Folder)
				if err != nil {
					return fmt.Errorf("%v %v", funcName, err.Error())
				}

				// Finish off the SQL files
				err = sqlwriter.FinishSQLFiles(sqlFiles)
				if err != nil {
					return fmt.Errorf("%v %v", funcName, err.Error())
				}

				// Checksums
				err = sqlwriter.CalculateChecksums(sqlFiles, config.ChecksumLog)
				if err != nil {
					return fmt.Errorf("%v %v", funcName, err.Error())
				}

				// Database Inserts
				if !config.SkipInserts {
					// Insert Into MySQL
					err = engine.DoInserts(config.DB.DBConfig, sqlFiles)
					if err != nil {
						return fmt.Errorf("%v %v", funcName, err.Error())
					}
				}
			}
		}
	}

	// Count the number of records processed
	rates.LogRecordsProcessed(config.TimingsLog, rateInfo)

	// Count the number of rows generated
	rates.LogRowsGenerated(config.TimingsLog, rateInfo)

	// Run Row Counts Checks
	database.LogRowCountChecks(config.DB.StorageEngine, config.DataFolder, rateInfo)

	// Calculate and log average rate
	rates.LogAvgRate(config.TimingsLog, rateInfo)

	// Calculate and log maximum rate
	rates.LogMaxRate(config.TimingsLog, rateInfo)

	// Calculate and log actual rate
	var duration time.Duration = time.Since(importStart)
	rates.LogActualRate(config.TimingsLog, rateInfo, duration)

	// Log import time
	var took time.Duration = timeutils.Took(importStart)
	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("Import took %v", took),
	)

	return nil
}
