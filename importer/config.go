package importer

import (
	"fmt"
	"io"

	"go-uk-maps-import/database/engine"
)

type Config struct {
	DataFolder    string
	ShapeFiles    []string
	NumShapeFiles int
	Download      bool
	Concurrent    bool
	Unlimited     bool
	SkipInserts   bool
	UseFiles      bool
	LowMemory     bool
	TimingsLog    io.Writer
	ChecksumLog   io.Writer
	DB            engine.SEConfig
	IsTest        bool
}

func (c Config) String() string {
	return fmt.Sprintf("\t\t"+"DataFolder: %v"+"\n"+
		"\t\t"+"NumShapeFiles: %v"+"\n"+
		"\t\t"+"Download: %v"+"\n"+
		"\t\t"+"Concurrent: %v"+"\n"+
		"\t\t"+"Unlimited: %v"+"\n"+
		"\t\t"+"SkipInserts: %v"+"\n"+
		"\t\t"+"UseFiles: %v"+"\n"+
		"\t\t"+"LowMemory: %v",
		c.DataFolder,
		c.NumShapeFiles,
		c.Download,
		c.Concurrent,
		c.Unlimited,
		c.SkipInserts,
		c.UseFiles,
		c.LowMemory,
	)
}
