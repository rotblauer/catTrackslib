package catTrackslib

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	catnames "github.com/rotblauer/cattracks-names"
)

type CatTrackGeoJSON struct {
	geojson.Feature
}

func (c CatTrackGeoJSON) String() string {
	alias := catnames.AliasOrSanitizedName(c.Properties["Name"].(string))
	dot := ""
	switch alias {
	case "rye":
		dot = "🔵"
	case "ia":
		dot = "🔴"
	}
	return fmt.Sprintf("%s Name: %s (%s), Coords: %v, Time: %v, Accuracy: %v, Speed: %v",
		dot,
		c.Properties["Name"],
		alias,
		c.Geometry.(orb.Point),
		c.Properties["Time"],
		c.Properties["Accuracy"],
		c.Properties["Speed"],
	)
}
