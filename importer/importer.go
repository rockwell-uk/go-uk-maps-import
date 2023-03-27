package importer

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rockwell-uk/csync/mutex"
	"github.com/rockwell-uk/csync/waitgroup"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/sliceutils"
	"github.com/rockwell-uk/go-utils/stringutils"
	"github.com/rockwell-uk/go-utils/timeutils"
	"github.com/rockwell-uk/shapefile"
	"github.com/rockwell-uk/uiprogress"

	"go-uk-maps-import/rates"
)

var (
	imported []string
	rateInfo rates.RatesInfo
)

func doImport(config Config) ([]rates.RateInfo, error) {
	var funcName string = "importer.doImport"
	var jobName string = "Import"

	var start time.Time = time.Now()
	var took time.Duration

	// Sequential
	if !config.Concurrent {
		logger.Log(
			logger.LVL_DEBUG,
			listFiles("shapefiles", config.DataFolder, config.ShapeFiles),
		)

		logger.Log(
			logger.LVL_APP,
			fmt.Sprintf("Importing Shapefiles Sequentially [%v]", config.NumShapeFiles),
		)

		if !config.Unlimited {
			// Process the shapefiles one at a time
			err := sequential(config, nil, config.ShapeFiles)
			if err != nil {
				return []rates.RateInfo{}, fmt.Errorf("%v: %v", funcName, err.Error())
			}
		} else {
			// Edge case - why would we set unlimited but not concurrent?
			var tasks []*progress.Task
			for _, shapeFile := range config.ShapeFiles {
				tasks = append(tasks, &progress.Task{
					ID:        shapeFile,
					Magnitude: float64(shapefile.GetRecordCount(shapeFile)),
				})
			}

			job := progress.SetupJob(jobName, tasks)
			defer job.End(true)

			// Process the shapefiles one at a time
			err := sequential(config, job, config.ShapeFiles)
			if err != nil {
				return []rates.RateInfo{}, fmt.Errorf("%v: %v", funcName, err.Error())
			}
		}
	}

	// Concurrent
	if config.Concurrent {
		// Unlimited
		if config.Unlimited {
			logger.Log(
				logger.LVL_APP,
				fmt.Sprintf("Importing All Shapefiles (Unlimited) [%v]", config.NumShapeFiles),
			)

			// Unlimited - Attempt to process every single shapefile at once
			logger.Log(
				logger.LVL_DEBUG,
				listFiles("shapefiles", config.DataFolder, config.ShapeFiles),
			)

			var tasks []*progress.Task
			for _, shapeFile := range config.ShapeFiles {
				tasks = append(tasks, &progress.Task{
					ID:        shapeFile,
					Magnitude: float64(shapefile.GetRecordCount(shapeFile)),
				})
			}

			job := progress.SetupJob(jobName, tasks)
			defer job.End(true)

			fanout(config, job, config.ShapeFiles)
		}

		// Not Unlimited
		if !config.Unlimited {
			// How many folders?
			folders, err := fileutils.Folders(config.DataFolder)
			if err != nil {
				return []rates.RateInfo{}, fmt.Errorf("%v: %v", funcName, err.Error())
			}

			if len(folders) > 0 {
				// Process each subfolder separately
				logger.Log(
					logger.LVL_DEBUG,
					"Finding subfolders:",
				)

				logger.Log(
					logger.LVL_DEBUG,
					listFiles("file", config.DataFolder, folders),
				)

				for _, folder := range folders {
					logger.Log(
						logger.LVL_DEBUG,
						fmt.Sprintf("Scanning folder '%v' for shapefiles\n", folder),
					)

					shapeFiles, err := getShapefilesInFolder(folder)
					if err != nil {
						return []rates.RateInfo{}, fmt.Errorf("%v: %v", funcName, err.Error())
					}

					logger.Log(
						logger.LVL_DEBUG,
						listFiles("shapefiles", config.DataFolder, shapeFiles),
					)

					if len(shapeFiles) > 0 {
						logger.Log(
							logger.LVL_APP,
							fmt.Sprintf("Importing [%v] Shapefiles from [%v]", len(shapeFiles), folder),
						)

						fanout(config, nil, shapeFiles)
					}
				}
			} else {
				// Process all
				logger.Log(
					logger.LVL_DEBUG,
					listFiles("shapefiles", config.DataFolder, config.ShapeFiles),
				)

				logger.Log(
					logger.LVL_APP,
					fmt.Sprintf("Importing Shapefiles [%v]", config.NumShapeFiles),
				)

				fanout(config, nil, config.ShapeFiles)
			}
		}
	}

	// Log import metrics etc.
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Rates: %v\n", rateInfo),
	)

	took = timeutils.Took(start)

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done Importing Shapefiles [%v]\n", took),
	)

	return rateInfo, nil
}

func fanout(config Config, job *progress.Job, shapeFiles []string) {
	var wg *waitgroup.WaitGroup = waitgroup.New()

	// Start progressbar if needed
	if progress.ShouldShowBar() && !config.Unlimited {
		uiprogress.Start()
	}

	for _, shapeFile := range shapeFiles {
		wg.Add(1)

		go func(sf string) {
			if job != nil {
				task, err := job.GetTask(sf)
				if err == nil {
					task.Start()
				}
			}
			importShapefile(config, sf) //nolint:errcheck
			if job != nil {
				task, err := job.GetTask(sf)
				if err == nil {
					task.End()
				}
				job.UpdateBar()
			}
			wg.Done()
		}(shapeFile)
	}

	wg.Wait()

	// Stop the progress bar before any more logging
	if progress.ShouldShowBar() && !config.Unlimited {
		uiprogress.Stop()
	}
}

func sequential(config Config, job *progress.Job, shapeFiles []string) error {
	var funcName string = "importer.sequential"

	// Start progressbar if needed
	if progress.ShouldShowBar() && !config.Unlimited {
		uiprogress.Start()
	}

	for _, shapeFile := range shapeFiles {
		if job != nil {
			task, err := job.GetTask(shapeFile)
			if err == nil {
				task.Start()
			}
		}

		err := importShapefile(config, shapeFile)
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}

		if job != nil {
			task, err := job.GetTask(shapeFile)
			if err == nil {
				task.End()
			}
			job.UpdateBar()
		}
	}

	// Stop the progress bar before any more logging
	if progress.ShouldShowBar() && !config.Unlimited {
		uiprogress.Stop()
	}

	return nil
}

func GetAllShapefiles(dataFolder string) ([]string, error) {
	var funcName string = "importer.GetAllShapefiles"

	// Check that the folder exists - (this should already be checked in configcheck)
	if !fileutils.FileExists(dataFolder) {
		e := fmt.Sprintf("the folder: %v does not exist\n", dataFolder)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)

		return []string{}, fmt.Errorf("%v: %v", funcName, e)
	}

	// Find all the shapefiles in the main, and subfolders of the data folder
	shapeFiles, err := getShapefilesInFolder(dataFolder)
	if err != nil {
		return []string{}, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	if len(shapeFiles) == 0 {
		e := fmt.Sprintf("no shapefiles were found in folder: %v, do you need to run with the -download flag?\n", dataFolder)
		logger.Log(
			logger.LVL_FATAL,
			stringutils.UcFirst(e),
		)

		return []string{}, fmt.Errorf("%v: %v", funcName, e)
	}

	return shapeFiles, nil
}

func getShapefilesInFolder(dataFolder string) ([]string, error) {
	var funcName string = "importer.getShapefiles"

	shapeFiles, err := fileutils.Find(dataFolder, ".shp")
	if err != nil {
		return []string{}, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return shapeFiles, nil
}

func importShapefile(config Config, shapeFile string) error {
	var funcName string = "importer.importShapefile"

	sfShortName := filepath.Base(shapeFile)

	recordsProcessed, rowsGenerated, timeTaken, err := doImportShapefile(
		config,
		shapeFile,
		sfShortName,
	)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	info := rates.RateInfo{
		ShapeFile: sfShortName,
		Records:   recordsProcessed,
		Rows:      rowsGenerated,
		Duration:  timeTaken,
	}

	mutex.Lock()
	rateInfo = append(rateInfo, info)
	imported = append(imported, shapeFile)
	mutex.Unlock()

	return nil
}

func listFiles(fileDesc string, dataFolder string, files []string) string {
	var list string
	var shortNames []string = make([]string, len(files))

	n := len(files)

	if n == 0 {
		return fmt.Sprintf("No %s found", fileDesc)
	}

	list = fmt.Sprintf("Found %v files:\n\t", n)

	for i, fullPath := range files {
		shortNames[i] = strings.ReplaceAll(fullPath, dataFolder, "")
	}

	return list + sliceutils.TabList(shortNames)
}
