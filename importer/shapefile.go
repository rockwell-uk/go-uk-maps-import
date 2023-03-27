package importer

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rockwell-uk/csync/waitgroup"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/stringutils"
	"github.com/rockwell-uk/shapefile"
	"github.com/rockwell-uk/shapefile/dbf"
	"github.com/rockwell-uk/shapefile/shp"
	"github.com/rockwell-uk/uiprogress"
	"github.com/twpayne/go-geos"

	"go-uk-maps-import/database"
	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/types"
	"go-uk-maps-import/filelogger"
	"go-uk-maps-import/osdata"
	"go-uk-maps-import/rates"
)

const (
	chunkSize     = 500
	ACTION_CHECK  = "CHECK"
	ACTION_INSERT = "INSERT"
	ACTION_DONE   = "DONE"
)

type importResult struct {
	rowsGenerated map[string]int
}

type importAction struct {
	action string
	insert insert
	shape  shp.Shape
}

type fieldName struct {
	fieldNames   string
	placeHolders string
}

func (m fieldName) String() string {
	return fmt.Sprintf("\n\tfieldNames:%v\n\tplaceHolders:%v\n", m.fieldNames, m.placeHolders)
}

var dbFieldsMap = make(map[string]fieldName)

func init() {
	for layerType, allFieldNames := range types.MapLayers {
		var fieldNames string
		var placeHolders string

		for _, field := range allFieldNames {
			fieldNames += fmt.Sprintf("%v, ", field)
			placeHolders += fmt.Sprintf(":%v, ", field)
		}

		fieldNames += fmt.Sprintf("%v", "ogc_geom")
		placeHolders += fmt.Sprintf("ST_GeomFromWKB(:%v)", "ogc_geom")

		dbFieldsMap[layerType] = fieldName{
			fieldNames,
			placeHolders,
		}
	}
}

func doImportShapefile(config Config, shapeFile, sfShortName string) (int, map[string]int, time.Duration, error) {
	var funcName string = "importer.doImportShapefile"
	var jobName string = "Importing shapefile"

	var importStarted = time.Now()
	var sfRowsGenerated = make(map[string]int)
	var recordsInFile uint32
	var recordsProcessed int

	// One context per shapefile
	var gctx *geos.Context = geos.NewContext()

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("%v [%v]\n", jobName, sfShortName),
	)

	var dbName = database.GetDBNameFromFilename(shapeFile)
	var rateInterval = 10000
	var barInterval = 1000

	var r *shapefile.Reader
	if !config.LowMemory {
		recordsInFile, r = shapefile.ReadShapeFileToMemory(shapeFile)
	} else {
		recordsInFile, r = shapefile.ReadShapeFile(shapeFile)
	}

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Records In File %v, [%v]\n", sfShortName, recordsInFile),
	)

	ops := make(chan importAction)
	res := make(chan importResult, 1)

	go func() {
		for f := range ops {
			switch f.action {
			case ACTION_INSERT:
				if config.UseFiles && *config.DB.Engine == engine.EngineMySQL {
					res <- importToFile(gctx, f, dbName)
				} else {
					res <- importDirect(gctx, f, dbName, sfShortName)
				}
			case ACTION_CHECK:
				if !config.UseFiles || *config.DB.Engine == engine.EngineSQLite {
					if recordsProcessed > 0 && recordsProcessed%chunkSize == 0 {
						runInserts(config.DB.StorageEngine, sfShortName)
					}
				}
				res <- importResult{}
			case ACTION_DONE:
				if !config.UseFiles || *config.DB.Engine == engine.EngineSQLite {
					runInserts(config.DB.StorageEngine, sfShortName)
				}
				res <- importResult{}
				return
			}
		}
	}()

	var magnitude int = int(math.Ceil(float64(recordsInFile) / float64(barInterval)))
	var wg *waitgroup.WaitGroup = waitgroup.New()

	var job *progress.Job
	var task *progress.Task
	var showBar bool = logger.Vbs == logger.LVL_APP && !config.Unlimited && !config.IsTest
	var currentChunk int

	// Progress bar
	if showBar {
		var tasks = []*progress.Task{}
		for i := 0; i < magnitude; i++ {
			tasks = append(tasks, &progress.Task{
				ID:        chunkName(i),
				Magnitude: 1, // each task is a single chunk
			})
		}

		job = progress.NewJob(jobName, len(tasks))
		job.AddTasks(tasks)

		job.Bar = uiprogress.AddBar(magnitude).PrependCompleted()
		job.Bar.AppendFunc(func(b *uiprogress.Bar) string {
			status, _ := job.GetStatus()
			return fmt.Sprintf("%s [%s]", stringutils.SpacePadRight(sfShortName, 34), status)
		})

		// N.b. do not defer the job end if unlimited
		if !config.Unlimited {
			defer job.End(false)
		}
		err := job.Start()
		if err != nil {
			return recordsProcessed, sfRowsGenerated, time.Since(importStarted), fmt.Errorf("%v: %v", funcName, err.Error())
		}

		// Start the job for the first chunk
		task, _ = job.GetTask(chunkName(currentChunk))
		task.Start()
	}

	for {
		rec, err := r.Next()

		var shouldLogRate bool = recordsProcessed > 0 && recordsProcessed%rateInterval == 0
		var shouldUpdateBar bool = recordsProcessed > 0 && recordsProcessed%barInterval == 0

		if err == nil {
			if recordsProcessed > 0 {
				ops <- importAction{
					action: ACTION_CHECK,
				}
				<-res
			}
		}

		if shouldLogRate {
			rate, rateLogLine := logRate(sfShortName, importStarted, recordsProcessed)
			if rate > 0 {
				logger.Log(
					logger.LVL_DEBUG,
					rateLogLine,
				)
			}
		}

		if showBar && shouldUpdateBar {
			if currentChunk == 0 {
				task.End()
			}
			currentChunk++

			wg.Add(1)

			task, _ := job.GetTask(chunkName(currentChunk))
			task.Start()

			go func(j *progress.Job, t *progress.Task) {
				t.End()
				j.UpdateBar()
				wg.Done()
			}(job, task)

			wg.Wait()
		}

		if errors.Is(err, io.EOF) {
			if showBar {
				wg.Add(1)

				go func(j *progress.Job, t *progress.Task) {
					t.End()
					j.UpdateBar()
					wg.Done()
				}(job, task)

				wg.Wait()
			}

			ops <- importAction{
				action: ACTION_DONE,
			}
			<-res

			_, rateLogLine := logRate(sfShortName, importStarted, recordsProcessed)
			filelogger.Log(
				filelogger.LogLine{
					File: config.TimingsLog,
					Line: rateLogLine,
				},
			)

			return recordsProcessed, sfRowsGenerated, time.Since(importStarted), nil
		} else if err != nil {
			logger.Log(
				logger.LVL_FATAL,
				err.Error(),
			)
			return recordsProcessed, sfRowsGenerated, time.Since(importStarted), fmt.Errorf("%v: %v", funcName, err.Error())
		}

		ops <- importAction{
			action: ACTION_INSERT,
			insert: getInsert(rec, r.Fields()),
			shape:  rec.Shape,
		}
		result := <-res

		sfRowsGenerated = mergeRowsGenerated(result.rowsGenerated, sfRowsGenerated)

		recordsProcessed++
	}
}

func mergeRowsGenerated(source map[string]int, dest map[string]int) map[string]int {
	for key, val := range source {
		dest[key] += val
	}

	return dest
}

func getInsert(rec *shapefile.Record, fields []*dbf.Field) insert {
	var record insert = insert{}

	for i, field := range fields {
		value := rec.Attr(i)

		// Fix invalid UTF8 strings
		value = osdata.InvalidUTF8Fix(value)

		// Fix for FEATCODE in "some" shapefiles
		if field.Name == "FEATCODE" {
			asInt := osdata.FeatcodeFix(value)
			record[field.Name] = asInt
		} else {
			record[field.Name] = value
		}
	}

	return record
}

func getRate(diff time.Duration, recordsProcessed int) float64 {
	if recordsProcessed == 0 {
		return 0
	}

	ns := float64(diff.Nanoseconds())

	return (float64(recordsProcessed) / ns) * 1000000000
}

func logRate(sfShortName string, importStarted time.Time, recordsProcessed int) (float64, string) {
	duration := time.Since(importStarted)
	var rate float64 = getRate(duration, recordsProcessed)
	rateLogLine := formatRateLogLine(sfShortName, duration, recordsProcessed)

	return rate, rateLogLine
}

func formatRateLogLine(sfShortName string, duration time.Duration, recordsProcessed int) string {
	rateInfo := rates.RateInfo{
		ShapeFile: sfShortName,
		Records:   recordsProcessed,
		Duration:  duration,
	}

	return fmt.Sprintf("%s avg/s [%s] processed %v records in %s",
		rateInfo.Rate(),
		sfShortName,
		recordsProcessed,
		rateInfo.Durn(),
	)
}

func chunkName(i int) string {
	return fmt.Sprintf("chunk_%v", i)
}
