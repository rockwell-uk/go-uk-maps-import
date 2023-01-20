package types

var FieldTypes = map[string]string{
	"ID":         "varchar(36) NOT NULL",
	"GRIDREF":    "smallint NOT NULL",
	"FEATCODE":   "double precision DEFAULT NULL",
	"CLASSIFICA": "varchar(255) DEFAULT NULL",
	"DISTNAME":   "varchar(255) DEFAULT NULL",
	"JUNCTNUM":   "varchar(10) DEFAULT NULL",
	"HTMLNAME":   "varchar(255) DEFAULT NULL",
	"FONTHEIGHT": "varchar(22) DEFAULT NULL",
	"ORIENTATIO": "varchar(10) DEFAULT NULL",
	"ROADNUMBER": "varchar(10) DEFAULT NULL",
	"DRAWLEVEL":  "varchar(10) DEFAULT NULL",
	"OVERRIDE":   "varchar(10) DEFAULT NULL",
	"HEIGHT":     "varchar(10) DEFAULT NULL",
}
