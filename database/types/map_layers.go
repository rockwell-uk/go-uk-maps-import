package types

import (
	"sort"
)

type LayerType []string

type LayerTypes map[string]LayerType

func (l LayerTypes) Ordered() []string {
	keys := make([]string, 0, len(l))

	for k := range l {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

var MapLayers = LayerTypes{
	"administrative_boundary":       {"ID", "GRIDREF", "CLASSIFICA", "FEATCODE"},
	"building":                      {"ID", "GRIDREF", "FEATCODE"},
	"electricity_transmission_line": {"ID", "GRIDREF", "FEATCODE"},
	"foreshore":                     {"ID", "GRIDREF", "FEATCODE"},
	"functional_site":               {"ID", "GRIDREF", "DISTNAME", "CLASSIFICA", "FEATCODE"},
	"glasshouse":                    {"ID", "GRIDREF", "FEATCODE"},
	"motorway_junction":             {"ID", "GRIDREF", "JUNCTNUM", "FEATCODE"},
	"named_place":                   {"ID", "GRIDREF", "DISTNAME", "HTMLNAME", "CLASSIFICA", "FONTHEIGHT", "ORIENTATIO", "FEATCODE"},
	"ornament":                      {"ID", "GRIDREF", "FEATCODE"},
	"railway_station":               {"ID", "GRIDREF", "DISTNAME", "CLASSIFICA", "FEATCODE"},
	"railway_track":                 {"ID", "GRIDREF", "CLASSIFICA", "FEATCODE"},
	"railway_tunnel":                {"ID", "GRIDREF", "FEATCODE"},
	"road_tunnel":                   {"ID", "GRIDREF", "FEATCODE"},
	"road":                          {"ID", "GRIDREF", "DISTNAME", "ROADNUMBER", "CLASSIFICA", "DRAWLEVEL", "OVERRIDE", "FEATCODE"},
	"roundabout":                    {"ID", "GRIDREF", "CLASSIFICA", "FEATCODE"},
	"spot_height":                   {"ID", "GRIDREF", "HEIGHT", "FEATCODE"},
	"surface_water_area":            {"ID", "GRIDREF", "FEATCODE"},
	"surface_water_line":            {"ID", "GRIDREF", "FEATCODE"},
	"tidal_boundary":                {"ID", "GRIDREF", "CLASSIFICA", "FEATCODE"},
	"tidal_water":                   {"ID", "GRIDREF", "FEATCODE"},
	"woodland":                      {"ID", "GRIDREF", "FEATCODE"},
}
