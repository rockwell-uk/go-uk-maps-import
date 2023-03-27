package database

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-sqlbench/benchmark"
	"github.com/rockwell-uk/go-utils/timeutils"
)

func Benchmark(db *sqlx.DB, dbDisplayName, tableParams string, numRows int) benchmark.BenchmarkResult {
	err := warmupEngine(db, dbDisplayName)
	if err != nil {
		return benchmark.BenchmarkResult{
			Err: err,
		}
	}

	return runBenchmark(db, dbDisplayName, tableParams, numRows)
}

func warmupEngine(db *sqlx.DB, dbDisplayName string) error {
	var funcName string = "database.warmupEngine"
	var taskName string = fmt.Sprintf("Warming up %v", dbDisplayName)

	var start time.Time = time.Now()
	var took time.Duration

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("%v\n", taskName),
	)

	bs := benchmark.BenchmarkSuite{
		WarmUp:     benchmark.Warmup,
		PrintStats: true,
	}

	if err := bs.WarmUp(db); err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", taskName, took),
	)

	return nil
}

func runBenchmark(db *sqlx.DB, dbDisplayName, tableParams string, numRows int) benchmark.BenchmarkResult {
	var taskName string = fmt.Sprintf("Benchmarking %v", dbDisplayName)

	var start time.Time = time.Now()
	var took time.Duration

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("%v\n", taskName),
	)

	b := benchmark.Benchmark{
		Name: fmt.Sprintf("%vBench", dbDisplayName),
		N:    numRows,
		Bm:   benchmark.BmWKBExec,
	}

	err := benchmark.SetupGeomBench(db, tableParams)
	if err != nil {
		return benchmark.BenchmarkResult{
			Err: err,
		}
	}

	result := b.Run(db)

	benchmark.TeardownGeomBench(db)

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", taskName, took),
	)

	return result
}
