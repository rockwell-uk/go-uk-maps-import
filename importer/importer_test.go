package importer

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/rockwell-uk/go-logger/logger"
	"github.com/rockwell-uk/go-utils/pointers"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/mysql"
	"go-uk-maps-import/filelogger"
	"go-uk-maps-import/sqlwriter"
)

func TestImporter(t *testing.T) {
	if os.Getenv("LIVETEST") != "true" {
		t.Skip("Skipping Importer test")
	}

	logVbs := logger.LVL_FATAL

	dbConfigMySQL := engine.SEConfig{
		Engine: pointers.StrPtr(engine.EngineMySQL),
		DBConfig: engine.DBConfig{
			Host:    pointers.StrPtr("127.0.0.1"),
			Port:    pointers.StrPtr("3307"),
			User:    pointers.StrPtr("osdata"),
			Pass:    pointers.StrPtr("osdata"),
			Schema:  pointers.StrPtr("osdata"),
			Timeout: pointers.IntPtr(10),
		},
		ClearDown: false,
	}

	dbConfigSQLite := engine.SEConfig{
		Engine: &engine.EngineSQLite,
	}

	dataFolder := "./testdata"

	shapefilesToImport, err := GetAllShapefiles(dataFolder)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		dbConfig       engine.SEConfig
		importerConfig Config
	}{
		"MySQL Sequential": {
			dbConfig: dbConfigMySQL,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  false,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    false,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"MySQL Concurrent": {
			dbConfig: dbConfigMySQL,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    false,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"MySQL Concurrent Unlimited": {
			dbConfig: dbConfigMySQL,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   true,
				SkipInserts: false,
				UseFiles:    false,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"MySQL Sequential UseFiles": {
			dbConfig: dbConfigMySQL,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  false,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    true,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"MySQL Concurrent UseFiles": {
			dbConfig: dbConfigMySQL,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    true,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"MySQL Concurrent Unlimited UseFiles": {
			dbConfig: dbConfigMySQL,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   true,
				SkipInserts: false,
				UseFiles:    true,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"SQLite Sequential": {
			dbConfig: dbConfigSQLite,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  false,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    false,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"SQLite Concurrent": {
			dbConfig: dbConfigSQLite,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    false,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"SQLite Concurrent Unlimited": {
			dbConfig: dbConfigSQLite,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   true,
				SkipInserts: false,
				UseFiles:    false,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"SQLite Sequential UseFiles": {
			dbConfig: dbConfigSQLite,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  false,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    true,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"SQLite Concurrent UseFiles": {
			dbConfig: dbConfigSQLite,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   false,
				SkipInserts: false,
				UseFiles:    true,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
		"SQLite Concurrent Unlimited UseFiles": {
			dbConfig: dbConfigSQLite,
			importerConfig: Config{
				DataFolder:  dataFolder,
				ShapeFiles:  shapefilesToImport,
				Download:    false,
				Concurrent:  true,
				Unlimited:   true,
				SkipInserts: false,
				UseFiles:    true,
				LowMemory:   false,
				TimingsLog:  io.Discard,
				ChecksumLog: io.Discard,
			},
		},
	}

	for name, tt := range tests {
		logger.Start(logVbs)
		filelogger.Start()
		sqlwriter.Start()

		err := engine.Startup(false, &tt.dbConfig)
		if err != nil {
			t.Fatal(err)
		}
		tt.importerConfig.DB = tt.dbConfig

		// pointless step for bizarre scenario using sqlite as intermediary
		// see runner.go line 70
		var m *mysql.MySQL
		if *tt.dbConfig.Engine == engine.EngineSQLite && tt.importerConfig.UseFiles {
			// Connect and prepare MySQL
			m := &mysql.MySQL{
				Config: mysql.MySQLConfig{
					Host:    *dbConfigMySQL.DBConfig.Host,
					Port:    *dbConfigMySQL.DBConfig.Port,
					User:    *dbConfigMySQL.DBConfig.User,
					Pass:    *dbConfigMySQL.DBConfig.Pass,
					Schema:  *dbConfigMySQL.DBConfig.Schema,
					Timeout: *dbConfigMySQL.DBConfig.Timeout,
				},
			}

			err := m.Connect()
			if err != nil {
				t.Fatal(err)
			}
		}

		rateInfo, err := doImport(tt.importerConfig)
		if err != nil {
			t.Fatal(err)
		}

		// cleanup
		if m != nil {
			err = m.Stop()
			if err != nil {
				t.Fatal(err)
			}
		}
		err = tt.importerConfig.DB.StorageEngine.Cleardown()
		if err != nil {
			t.Fatal(err)
		}
		err = engine.Shutdown(false, tt.dbConfig)
		if err != nil {
			t.Fatal(err)
		}

		// shutdown
		sqlwriter.Stop()
		filelogger.Stop()
		logger.Stop()

		wantedString := "Records 64 Rows map[sd:64]"

		if !strings.Contains(rateInfo[0].String(), wantedString) {
			t.Fatalf("%v: Expected [%v] to contain [%v]", name, rateInfo[0].String(), wantedString)
		}
	}
}

func TestGetFieldNamesAndValues(t *testing.T) {
	tests := []struct {
		input       insert
		fieldNames  string
		fieldValues string
	}{
		{
			input: insert{
				"ID":       "196D2113-10D7-48F8-A3C4-432A40B1AFA3",
				"FEATCODE": 25200,
			},
			fieldNames:  "FEATCODE, ID, ",
			fieldValues: `25200, "196D2113-10D7-48F8-A3C4-432A40B1AFA3", `,
		},
		{
			input: insert{
				"ID":       "196D2113-10D7-48F8-A3C4-432A40B1AFA3",
				"FEATCODE": 25200.0000,
			},
			fieldNames:  "FEATCODE, ID, ",
			fieldValues: `25200, "196D2113-10D7-48F8-A3C4-432A40B1AFA3", `,
		},
	}

	for _, tt := range tests {
		fieldNames, fieldValues := getFieldNamesAndValues(tt.input)

		if tt.fieldNames != fieldNames {
			t.Fatalf("fieldNames: expected [%v], got [%v]", tt.fieldNames, fieldNames)
		}

		if tt.fieldValues != fieldValues {
			t.Fatalf("fieldValues: expected [%v], got [%v]", tt.fieldValues, fieldValues)
		}
	}
}
