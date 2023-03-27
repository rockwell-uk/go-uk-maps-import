package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/stringutils"
)

const (
	usePagePerStepsTimeoutSeconds int64 = 30
)

func doBackup(srcDb *sqlx.DB, srcDbConn *sqlite3.SQLiteConn, srcDbName, destFilename string, usePerPageSteps bool) error {
	var funcName string = "sqlite.doBackup"

	var e string

	// This function will be called multiple times.
	// It uses sql.Register(), which requires the name parameter value to be unique.
	// There does not currently appear to be a way to unregister a registered driver, however.
	// So generate a database driver name that will likely be unique.
	driverName := fmt.Sprintf("sqlite3_backup_%v", time.Now().UnixNano())

	var destDBConn *sqlite3.SQLiteConn
	sql.Register(driverName,
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				destDBConn = conn
				return nil
			},
		})

	// Connect to the destination database.
	destDB, err := sql.Open(driverName, destFilename)
	if err != nil {
		e = fmt.Sprintln("failed to open the destination database:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}
	defer destDB.Close()

	err = destDB.Ping()
	if err != nil {
		e = fmt.Sprintln("failed to connect to the destination database:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Check the driver connections.
	if srcDbConn == nil {
		e = "the source database driver connection is nil"
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}
	if destDBConn == nil {
		e = "the destination database driver connection is nil"
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Determining how many table will be backed up.
	var srcTableCount int
	err = srcDb.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").Scan(&srcTableCount)
	if err != nil {
		e = fmt.Sprintln("failed to check the source table count:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Found %v table(s) in the source database [%v]", srcTableCount, srcDbName),
	)

	// Confirm that the destination database is initially empty.
	var destTableCount int
	err = destDB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").Scan(&destTableCount)
	if err != nil {
		e = fmt.Sprintln("failed to check the destination table count:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}
	if destTableCount != 0 {
		e = fmt.Sprintf("the destination database is not empty; %v table(s) found", destTableCount)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Prepare to perform the backup.
	backup, err := destDBConn.Backup("main", srcDbConn, "main")
	if err != nil {
		e = fmt.Sprintln("failed to initialize the backup:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Allow the initial page count and remaining values to be retrieved.
	// According to <https://www.sqlite.org/c3ref/backup_finish.html>, the page count and remaining values are "... only updated by sqlite3_backup_step()."
	isDone, err := backup.Step(0)
	if err != nil {
		e = fmt.Sprintln("unable to perform an initial 0-page backup step:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}
	if isDone {
		e = "backup is unexpectedly done"
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Check that the page count and remaining values are reasonable.
	initialPageCount := backup.PageCount()
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Initial page count %v\n", initialPageCount),
	)
	if initialPageCount <= 0 {
		e = fmt.Sprintf("unexpected initial page count value: %v", initialPageCount)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	initialRemaining := backup.Remaining()
	if initialRemaining <= 0 {
		e = fmt.Sprintf("unexpected initial remaining value: %v", initialRemaining)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}
	if initialRemaining != initialPageCount {
		e = fmt.Sprintf("initial remaining value differs from the initial page count value; remaining: %v; page count: %v", initialRemaining, initialPageCount)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Perform the backup.
	if usePerPageSteps {
		startTime := time.Now().Unix()

		// Test backing-up using a page-by-page approach.
		latestRemaining := initialRemaining
		for {
			// Perform the backup step.
			isDone, err = backup.Step(1)
			if err != nil {
				e = fmt.Sprintln("failed to perform a backup step:", err)
				logger.Log(
					logger.LVL_FATAL,
					stringutils.UcFirst(e),
				)
				return fmt.Errorf("%v: %v", funcName, e)
			}

			// The page count should remain unchanged from its initial value.
			currentPageCount := backup.PageCount()
			if currentPageCount != initialPageCount {
				e = fmt.Sprintf("current page count differs from the initial page count; initial page count: %v; current page count: %v", initialPageCount, currentPageCount)
				logger.Log(
					logger.LVL_FATAL,
					stringutils.UcFirst(e),
				)
				return fmt.Errorf("%v: %v", funcName, e)
			}

			// There should now be one less page remaining.
			currentRemaining := backup.Remaining()
			expectedRemaining := latestRemaining - 1
			if currentRemaining != expectedRemaining {
				e = fmt.Sprintf("unexpected remaining value; expected remaining value: %v; actual remaining value: %v", expectedRemaining, currentRemaining)
				logger.Log(
					logger.LVL_FATAL,
					stringutils.UcFirst(e),
				)
				return fmt.Errorf("%v: %v", funcName, e)
			}
			latestRemaining = currentRemaining

			if isDone {
				break
			}

			// Limit the runtime of the backup attempt.
			if (time.Now().Unix() - startTime) > usePagePerStepsTimeoutSeconds {
				e = "backup is taking longer than expected"
				logger.Log(
					logger.LVL_FATAL,
					stringutils.UcFirst(e),
				)
				return fmt.Errorf("%v: %v", funcName, e)
			}
		}
	} else {
		// Test the copying of all remaining pages.
		isDone, err = backup.Step(-1)
		if err != nil {
			e = fmt.Sprintln("failed to perform a backup step:", err)
			logger.Log(
				logger.LVL_FATAL,
				stringutils.UcFirst(e),
			)
			return fmt.Errorf("%v: %v", funcName, e)
		}
		if !isDone {
			e = "backup is unexpectedly not done"
			logger.Log(
				logger.LVL_FATAL,
				stringutils.UcFirst(e),
			)
			return fmt.Errorf("%v: %v", funcName, e)
		}
	}

	// Check that the page count and remaining values are reasonable.
	finalPageCount := backup.PageCount()
	if finalPageCount != initialPageCount {
		e = fmt.Sprintf("final page count differs from the initial page count; initial page count: %v; final page count: %v", initialPageCount, finalPageCount)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	finalRemaining := backup.Remaining()
	if finalRemaining != 0 {
		e = fmt.Sprintf("unexpected remaining value: %v", finalRemaining)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	// Finish the backup.
	err = backup.Finish()
	if err != nil {
		e = fmt.Sprintln("failed to finish backup:", err)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)
		return fmt.Errorf("%v: %v", funcName, e)
	}

	return nil
}
