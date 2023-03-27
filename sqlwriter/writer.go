package sqlwriter

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/rockwell-uk/go-utils/fileutils"
)

type SQLLine struct {
	DBName string
	Table  string
	Line   string
}

var (
	Folder = "sql"
	done   chan struct{}
	lines  chan SQLLine
)

func Start() {
	log.SetFlags(0)

	done = make(chan struct{})
	lines = make(chan SQLLine, 1000)

	prepOutputFolder()

	go monitorLoop()
}

func Stop() {
	close(lines)
	<-done
}

func Write(l SQLLine) {
	lines <- l
}

func prepOutputFolder() {
	err := fileutils.MkDir(Folder)
	if err != nil {
		panic(err)
	}

	err = fileutils.EmptyFolder(Folder)
	if err != nil {
		panic(err)
	}
}

func monitorLoop() {
	for l := range lines {
		f, _ := getSQLFile(l.DBName, l.Table)
		if _, err := io.WriteString(f, l.Line+"\n"); err != nil {
			panic(err)
		}
		f.Close()
	}

	close(done)
}

func getSQLFile(dbName, tableName string) (*os.File, error) {
	var funcName string = "sqlwriter.getSQLFile"

	folderPath := fmt.Sprintf("%s/%s", Folder, dbName)
	err := fileutils.MkDir(folderPath)
	if err != nil {
		panic(err)
	}

	sqlFile := getSQLFilePath(dbName, tableName)
	f, err := fileutils.GetFile(sqlFile)
	if err != nil {
		return nil, fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return f, nil
}

func getSQLFilePath(dbName, fileName string) string {
	fPath := fmt.Sprintf("%s/%s", Folder, dbName)
	return fmt.Sprintf("%s/%s.sql", fPath, fileName)
}
