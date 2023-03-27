package sqlwriter

import (
	"fmt"
	"os"
	"strings"
)

func removeLastComma(file string) error {
	var funcName string = "sqlwriter.removeLastComma"

	f, err := os.OpenFile(file, os.O_RDWR, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	var nb int64 = 2
	block := make([]byte, nb)
	_, err = f.ReadAt(block, stat.Size()-nb)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}
	lastChars := string(block)

	lastChars = strings.Replace(lastChars, ",", "", 1)
	_, err = f.WriteAt([]byte(lastChars), stat.Size()-nb)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	err = f.Truncate(stat.Size() - 1)
	if err != nil {
		return fmt.Errorf("%v: %v", funcName, err.Error())
	}

	return nil
}
