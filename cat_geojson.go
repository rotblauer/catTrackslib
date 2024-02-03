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
	return fmt.Sprintf("Name: %s (%s), Coords: %v, Time: %v, Accuracy: %v, Speed: %v",
		c.Properties["Name"],
		catnames.AliasOrSanitizedName(c.Properties["Name"].(string)),
		c.Geometry.(orb.Point),
		c.Properties["Time"],
		c.Properties["Accuracy"],
		c.Properties["Speed"],
	)
}
