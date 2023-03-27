package importer

import (
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/rockwell-uk/go-logger/logger"

	"go-uk-maps-import/database/engine"
	"go-uk-maps-import/database/engine/mysql"
	"go-uk-maps-import/filelogger"
	"go-uk-maps-import/sqlwriter"
)

func BenchmarkImportShapeDirect(b *testing.B) {
	logger.Start(logger.LVL_FATAL)
	filelogger.Start()

	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	config := Config{
		TimingsLog: io.Discard,
		DB: engine.SEConfig{
			StorageEngine: &mysql.MySQL{
				DB: sqlxDB,
			},
		},
		IsTest: true,
	}

	sf := "./testdata/SD_MotorwayJunction.shp"
	sfsn := "SD_MotorwayJunction.shp"
	replaceQueryRgx := `REPLACE INTO motorway_junction.sd \((.+), (.+), (.+), (.+), ogc_geom\) VALUES \((.+), (.+), (.+), (.+), (.+)\)`
	numQueries := 1

	for i := 0; i < b.N; i++ {
		for n := 0; n < numQueries; n++ {
			mock.ExpectExec(replaceQueryRgx).WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
		}

		_, _, _, err := doImportShapefile(config, sf, sfsn)
		if err != nil {
			b.Fatal(err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkImportShapeToFile(b *testing.B) {
	logger.Start(logger.LVL_FATAL)
	filelogger.Start()
	sqlwriter.Start()

	sf := "./testdata/SD_MotorwayJunction.shp"
	sfsn := "SD_MotorwayJunction.shp"

	config := Config{
		TimingsLog: io.Discard,
		UseFiles:   true,
		IsTest:     true,
	}

	for i := 0; i < b.N; i++ {
		_, _, _, err := doImportShapefile(config, sf, sfsn)
		if err != nil {
			b.Fatal(err)
		}
	}
}
