package database

import (
	"reflect"
	"testing"
)

func TestGetDBNameFromFilename(t *testing.T) {
	tests := map[string]string{
		"TG_Building.shp":                    "building",
		"TG_ElectricityTransmissionLine.shp": "electricity_transmission_line",
		"TG_Foreshore.shp":                   "foreshore",
		"TG_FunctionalSite.shp":              "functional_site",
		"TG_Glasshouse.shp":                  "glasshouse",
		"TG_NamedPlace.shp":                  "named_place",
		"TG_Ornament.shp":                    "ornament",
		"TG_RailwayStation.shp":              "railway_station",
		"TG_RailwayTrack.shp":                "railway_track",
		"TG_RailwayTunnel.shp":               "railway_tunnel",
		"TG_Road.shp":                        "road",
		"TG_RoadTunnel.shp":                  "road_tunnel",
		"TG_Roundabout.shp":                  "roundabout",
		"TG_SpotHeight.shp":                  "spot_height",
		"TG_SurfaceWater_Area.shp":           "surface_water_area",
		"TG_SurfaceWater_Line.shp":           "surface_water_line",
		"TG_TidalBoundary.shp":               "tidal_boundary",
		"TG_TidalWater.shp":                  "tidal_water",
		"TG_Woodland.shp":                    "woodland",
	}

	for input, expected := range tests {
		actual := GetDBNameFromFilename(input)

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected [%+v]\nGot [%+v]", expected, actual)
		}
	}
}
