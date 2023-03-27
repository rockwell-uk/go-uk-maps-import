package osdata

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/timeutils"
)

const (
	osDataAPI = "https://api.os.uk/downloads/v1/products/VectorMapDistrict/downloads"
	zipDir    = "./resources/mapdata-source-files/zip"
	shpDir    = "./resources/mapdata-source-files/shp"
)

func DownloadVectorMapDistrict() error {
	var funcName string = "osdata.DownloadVectorMapDistrict"

	var tiles []VectorMapDistrictTile

	logger.Log(
		logger.LVL_DEBUG,
		"Starting the download process",
	)

	// Get the list of VectorMapDistrictTile (download list)
	tiles, err := getDownloadList()
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	// Filter the download list
	tiles = filterDownloadList(tiles)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	// Make the necessary folders
	err = prepFolders()
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("%v files to download\n", len(tiles)),
	)

	// Download the tiles
	err = doDownloadJob(tiles)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("%v files to unzip\n", len(tiles)),
	)

	// Unzip the downloaded tiles
	err = doUnzipJob(tiles)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	logger.Log(
		logger.LVL_DEBUG,
		"Completed the download process",
	)

	return nil
}

func doDownloadJob(tiles []VectorMapDistrictTile) error {
	var funcName string = "osdata.doDownloadJob"
	var jobName string = "Downloading OSData Source Files"

	var magnitude int = len(tiles)
	var start time.Time = time.Now()
	var took time.Duration

	if magnitude > 0 {
		logger.Log(
			logger.LVL_APP,
			fmt.Sprintf("%v [%v]\n", jobName, magnitude),
		)
	}

	// tile.Area is the nationalgrid square
	for _, tile := range tiles {
		err := downloadTile(tile)
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}
	}

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", jobName, took),
	)

	return nil
}

func doUnzipJob(tiles []VectorMapDistrictTile) error {
	var funcName string = "osdata.doUnzipJob"
	var jobName string = "Unzipping OSData Source Files"

	var magnitude int = len(tiles)
	var start time.Time = time.Now()
	var took time.Duration

	if magnitude > 0 {
		logger.Log(
			logger.LVL_APP,
			fmt.Sprintf("%v [%v]\n", jobName, magnitude),
		)
	}

	for _, tile := range tiles {
		err := unzipTile(tile)
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}
	}

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", jobName, took),
	)

	return nil
}

func getDownloadList() ([]VectorMapDistrictTile, error) {
	var funcName string = "osdata.getDownloadList"

	var tiles = []VectorMapDistrictTile{}

	logger.Log(
		logger.LVL_DEBUG,
		"Retrieving VectorMapDistrict tiles list",
	)

	resp, cancel, err := fileutils.Get(osDataAPI)
	defer cancel()
	if err != nil {
		return []VectorMapDistrictTile{}, fmt.Errorf("%v: no response from request to %v [%v]", funcName, osDataAPI, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return []VectorMapDistrictTile{}, fmt.Errorf("%v: unable to read response body [%v]", funcName, err.Error())
	}

	if err := json.Unmarshal(body, &tiles); err != nil {
		return []VectorMapDistrictTile{}, fmt.Errorf("%v: cannot unmarshal JSON [%v]", funcName, err.Error())
	}

	logger.Log(
		logger.LVL_INTERNAL,
		fmt.Sprintf("VectorMapDistrictTiles %v\n", tiles),
	)

	return tiles, nil
}

func filterDownloadList(downloadList []VectorMapDistrictTile) []VectorMapDistrictTile {
	var funcName string = "osdata.filterDownloadList"

	filteredDownloadList := []VectorMapDistrictTile{}

	// Initial filter
	for _, tile := range downloadList {
		// We only want ESRI® Shapefiles, and not tile GB
		if tile.Format == "ESRI® Shapefile" && tile.Area != "GB" {
			filteredDownloadList = append(filteredDownloadList, tile)
		}
	}

	tilesToDownload := []VectorMapDistrictTile{}

	// Check for existing files and check md5 hash
	for _, tile := range filteredDownloadList {
		out := fmt.Sprintf("%v/%v", zipDir, tile.FileName)

		md5Hash, err := fileutils.GetMD5Hash(out)
		if err != nil {
			logger.Log(
				logger.LVL_INTERNAL,
				fmt.Sprintf("%v: %v does not already exist [%v]\n", funcName, out, err.Error()),
			)
			tilesToDownload = append(tilesToDownload, tile)
			continue
		}

		if md5Hash != tile.MD5 {
			logger.Log(
				logger.LVL_WARN,
				fmt.Sprintf("md5 hash does not match %v [%v:%v]\n", tile.FileName, md5Hash, tile.MD5),
			)
			tilesToDownload = append(tilesToDownload, tile)
		}
	}

	return tilesToDownload
}

func prepFolders() error {
	var funcName string = "osdata.prepFolders"

	err := fileutils.MkDir(zipDir)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	// remove any old temp files
	tmpFiles, err := fileutils.Find(zipDir, ".tmp")
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}
	for _, tmpFile := range tmpFiles {
		logger.Log(
			logger.LVL_INTERNAL,
			fmt.Sprintf("%v: removing temp file [%v]\n", funcName, tmpFile),
		)
		err = os.RemoveAll(tmpFile)
		if err != nil {
			return fmt.Errorf("%v: %v", funcName, err.Error())
		}
	}

	err = fileutils.MkDir(shpDir)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return nil
}
