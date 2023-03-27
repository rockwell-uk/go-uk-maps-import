package database

import (
	"strings"

	"github.com/rockwell-uk/go-utils/fileutils"
	"github.com/rockwell-uk/go-utils/stringutils"
)

func GetDBNameFromFilename(filename string) string {
	var res string

	var file string = fileutils.FileNameWithoutExtension(filename)
	p := strings.Split(file, "_")

	for i := 1; i < len(p); i++ {
		if i > 1 {
			res += "_"
		}
		res += stringutils.ToSnakeCase(p[i])
	}

	return res
}

func GetSquareFilename(filename string) string {
	var file string = fileutils.FileNameWithoutExtension(filename)

	p := strings.Split(file, "_")

	return strings.ToLower(p[0])
}
