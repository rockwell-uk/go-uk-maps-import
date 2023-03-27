package osdata

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/rockwell-uk/csync/waitgroup"
	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-progress/progress"
	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/stringutils"
	"github.com/rockwell-uk/uiprogress"
)

type WriteCounter struct {
	Total uint64
	Job   *progress.Job
}

func (wc *WriteCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.Total += uint64(n)
	wc.Job.SetBar(int(wc.Total))
	return n, nil
}

func downloadTile(tile VectorMapDistrictTile) error {
	var funcName string = "osdata.downloadTile"

	var magnitude int = tile.Size
	var fileName string = tile.FileName
	var wg *waitgroup.WaitGroup = waitgroup.New()
	var units string = "mb"
	var out string = fmt.Sprintf("%v/%v", zipDir, tile.FileName)
	var fileSize float64 = fileutils.ByteSizeConvert(int64(magnitude), units)

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Downloading %v [%.2f%v]\n", tile.FileName, fileSize, units),
	)

	var task = &progress.Task{
		ID:        fileName,
		Magnitude: float64(magnitude),
	}

	var tasks = []*progress.Task{
		task,
	}

	job := progress.NewJob(fileName, len(tasks))
	job.AddTasks(tasks)

	if progress.ShouldShowBar() {
		uiprogress.Start()

		job.Bar = uiprogress.AddBar(magnitude).PrependCompleted()
		job.Bar.AppendFunc(func(b *uiprogress.Bar) string {
			status, _ := job.GetStatus()
			var taskName string = fmt.Sprintf("%v %.2f%v", fileName, fileSize, units)
			return fmt.Sprintf("%s [%s]", stringutils.SpacePadRight(taskName, 22), status)
		})
		defer job.End(false)

		err := job.Start()
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}

		task.Start()
		wg.Add(1)

		c := make(chan error)
		go func(j *progress.Job, t *progress.Task, vt VectorMapDistrictTile, o string) {
			counter := &WriteCounter{
				Job: j,
			}
			err := download(o, vt.URL, counter)
			if err != nil {
				c <- err
			}
			t.End()
			wg.Done()

			c <- nil
		}(job, task, tile, out)

		err = <-c
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}

		wg.Wait()

		uiprogress.Stop()
	} else {
		err := download(out, tile.URL, &WriteCounter{})
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}
	}

	return nil
}

func download(filepath string, url string, counter *WriteCounter) error {
	var funcName string = "osdata.download"

	var tmpFile string = fmt.Sprintf("%v%v", filepath, ".tmp")

	out, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	resp, cancel, err := fileutils.Request(url, http.MethodGet, nil, nil)
	defer cancel()
	if err != nil {
		out.Close()
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}
	defer resp.Body.Close()

	if _, err = io.Copy(out, io.TeeReader(resp.Body, counter)); err != nil {
		out.Close()
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	out.Close()

	if err = os.Rename(tmpFile, filepath); err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return nil
}
