package catTrackslib

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// TrackPoint Stores a snippet of life, love, and location
type TrackPoint struct {
	Uuid            string    `json:"uuid"`
	PushToken       string    `json:"pushToken"`
	Version         string    `json:"version"`
	ID              int64     `json:"id"` // either bolt auto id or unixnano //think nano is better cuz can check for dupery
	Name            string    `json:"name"`
	Lat             float64   `json:"lat"`
	Lng             float64   `json:"long"`
	Accuracy        float64   `json:"accuracy"`       // horizontal, in meters
	VAccuracy       float64   `json:"vAccuracy"`      // vertical, in meteres
	Elevation       float64   `json:"elevation"`      // in meters
	Speed           float64   `json:"speed"`          // in m/s
	SpeedAccuracy   float64   `json:"speed_accuracy"` // in meters per second
	Tilt            float64   `json:"tilt"`           // degrees?
	Heading         float64   `json:"heading"`        // in degrees
	HeadingAccuracy float64   `json:"heading_accuracy"`
	HeartRate       float64   `json:"heartrate"` // bpm
	Time            time.Time `json:"time"`
	Floor           int       `json:"floor"` // building floor if available
	Notes           string    `json:"notes"` // special events of the day
	COVerified      bool      `json:"COVerified"`
	RemoteAddr      string    `json:"remoteaddr"`
}

type TrackPoints []*TrackPoint

// https://stackoverflow.com/questions/18390266/how-can-we-truncate-float64-type-to-a-particular-precision
func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

// TrackToFeature2 (WIP/experimental) is a track->geojson function that uses reflection to
// transfer fields. This might be useful for a more dynamic approach to geojson, but it's
// probably better in the broader scheme to just swap trackpoints for geojson entirely, though
// this would require coordinated changes between the client (cattracker) and server.
func TrackToFeature2(tp *TrackPoint) *geojson.Feature {
	if tp == nil {
		return nil
	}

	// config
	var timeFormat = time.RFC3339

	p := geojson.NewFeature(orb.Point{tp.Lng, tp.Lat})
	props := make(map[string]interface{})

	tpV := reflect.ValueOf(*tp)
	typeOfS := tpV.Type()

	stringSliceContains := func(s []string, e string) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	skipTrackFields := []string{"Lat", "Lng", "PushToken", "Version", "COVerified", "RemoteAddr", "Notes"}

	for i := 0; i < tpV.NumField(); i++ {
		fieldName := typeOfS.Field(i).Name
		if stringSliceContains(skipTrackFields, fieldName) {
			continue
		}
		switch t := tpV.Field(i).Interface().(type) {
		case time.Time:
			props[typeOfS.Field(i).Name] = t.Format(timeFormat)
		case float64:
			props[typeOfS.Field(i).Name] = toFixed(t, 2)
		case string:
			if t != "" {
				props[typeOfS.Field(i).Name] = t
			}
		default:
			props[typeOfS.Field(i).Name] = tpV.Field(i).Interface()
		}
	}

	if ns, e := NotesField(tp.Notes).AsNoteStructured(); e == nil {
		tpN := reflect.ValueOf(ns)
		typeOfN := tpN.Type()

		if ns.HasValidVisit() {
			props["Visit"] = ns.Visit
		}
		skipNoteFields := []string{"Visit", "NetworkInfo"}
		for i := 0; i < tpN.NumField(); i++ {
			if stringSliceContains(skipNoteFields, typeOfN.Field(i).Name) {
				continue
			}
			switch t := tpN.Field(i).Interface().(type) {
			case time.Time:
				props[typeOfN.Field(i).Name] = t.Format(timeFormat)
			case float64:
				props[typeOfN.Field(i).Name] = toFixed(t, 2)
			case string:
				if t != "" {
					props[typeOfN.Field(i).Name] = t
				}
			default:
				props[typeOfN.Field(i).Name] = tpN.Field(i).Interface()
			}
		}
	}

	p.Properties = props
	return p
}

// TrackToFeature converts a TrackPoint to a GeoJSON feature.
func TrackToFeature(trackPointCurrent *TrackPoint) *geojson.Feature {
	p := geojson.NewFeature(orb.Point{trackPointCurrent.Lng, trackPointCurrent.Lat})

	// currently need speed, name,time
	props := make(map[string]interface{})
	defer func() {
		p.Properties = props
	}()
	props["UUID"] = trackPointCurrent.Uuid
	props["Name"] = trackPointCurrent.Name
	props["Time"] = trackPointCurrent.Time
	props["UnixTime"] = trackPointCurrent.Time.Unix()
	props["Version"] = trackPointCurrent.Version
	props["Speed"] = toFixed(trackPointCurrent.Speed, 3)
	props["Elevation"] = toFixed(trackPointCurrent.Elevation, 2)
	props["Heading"] = toFixed(trackPointCurrent.Heading, 1)
	props["Accuracy"] = toFixed(trackPointCurrent.Accuracy, 2)

	if trackPointCurrent.VAccuracy > 0 {
		props["vAccuracy"] = trackPointCurrent.VAccuracy
	}
	if trackPointCurrent.SpeedAccuracy > 0 {
		props["speed_accuracy"] = trackPointCurrent.SpeedAccuracy
	}
	if trackPointCurrent.HeadingAccuracy > 0 {
		props["heading_accuracy"] = trackPointCurrent.HeadingAccuracy
	}

	// not implemented yet
	if hr := trackPointCurrent.HeartRate; hr != 0 {
		props["HeartRate"] = hr
	}

	if ns, e := NotesField(trackPointCurrent.Notes).AsNoteStructured(); e == nil {
		props["Activity"] = ns.Activity

		if v := ns.ActivityConfidence; v != nil {
			props["ActivityConfidence"] = *v
		}

		props["Pressure"] = toFixed(ns.Pressure, 2)
		if ns.CustomNote != "" {
			props["Notes"] = ns.CustomNote
		}
		if ns.ImgS3 != "" {
			props["imgS3"] = ns.ImgS3
		}
		if ns.HasRawImage() {
			props["imgB64"] = ns.ImgB64
		}
		if ns.HasValidVisit() {
			// TODO: ok to use mappy sub interface here?
			props["Visit"] = ns.Visit
		}

		if trackPointCurrent.HeartRate == 0 {
			if i := ns.HeartRateI(); i > 0 {
				props["HeartRate"] = toFixed(i, 2)
			}
		}

		// these properties might exist in the track, but we haven't been dumping them to json,
		// they're not deal breakers, but nice to have
		if ns.NumberOfSteps > 0 {
			props["NumberOfSteps"] = ns.NumberOfSteps
		}
		if ns.AverageActivePace > 0 {
			props["AverageActivePace"] = toFixed(ns.AverageActivePace, 2)
		}
		if ns.CurrentPace > 0 {
			props["CurrentPace"] = toFixed(ns.CurrentPace, 2)
		}
		if ns.CurrentCadence > 0 {
			props["CurrentCadence"] = toFixed(ns.CurrentCadence, 2)
		}
		if ns.CustomNote != "" {
			props["CustomNote"] = ns.CustomNote
		}
		if ns.FloorsAscended > 0 {
			props["FloorsAscended"] = ns.FloorsAscended
		}
		if ns.FloorsDescended > 0 {
			props["FloorsDescended"] = ns.FloorsDescended
		}
		if !ns.CurrentTripStart.IsZero() {
			props["CurrentTripStart"] = ns.CurrentTripStart
		}
		if ns.Distance > 0 {
			props["Distance"] = toFixed(ns.Distance, 2)
		}

		if ns.Lightmeter > 0 {
			props["Lightmeter"] = toFixed(ns.Lightmeter, 2)
		}
		if ns.AmbientTemp > 0 {
			props["AmbientTemp"] = toFixed(ns.AmbientTemp, 2)
		}
		if ns.Humidity > 0 {
			props["Humidity"] = toFixed(ns.Humidity, 2)
		}
		if v := ns.Accelerometer.X; v != nil {
			props["AccelerometerX"] = *v
		}
		if v := ns.Accelerometer.Y; v != nil {
			props["AccelerometerY"] = *v
		}
		if v := ns.Accelerometer.Z; v != nil {
			props["AccelerometerZ"] = *v
		}
		if v := ns.UserAccelerometer.X; v != nil {
			props["UserAccelerometerX"] = *v
		}
		if v := ns.UserAccelerometer.Y; v != nil {
			props["UserAccelerometerY"] = *v
		}
		if v := ns.UserAccelerometer.Z; v != nil {
			props["UserAccelerometerZ"] = *v
		}
		if v := ns.Gyroscope.X; v != nil {
			props["GyroscopeX"] = *v
		}
		if v := ns.Gyroscope.Y; v != nil {
			props["GyroscopeY"] = *v
		}
		if v := ns.Gyroscope.Z; v != nil {
			props["GyroscopeZ"] = *v
		}
		if v := ns.BatteryStatus; v != "" {
			bs := BatteryStatus{}
			if err := json.Unmarshal([]byte(v), &bs); err == nil {
				props["BatteryStatus"] = bs.Status
				props["BatteryLevel"] = toFixed(bs.Level, 2)
			}
		}
		if v := ns.NetworkInfo; v != "" {
			props["NetworkInfo"] = v
		}

		// if trackPointCurrent.HeartRate == 0 && ns.HeartRateType != "" {
		// 	props["HeartRateType"] = ns.HeartRateType
		// }

	} else if _, e := NotesField(trackPointCurrent.Notes).AsFingerprint(); e == nil {
		// maybe do something with identity consolidation?
	} else {
		// NOOP normal
		// props["Notes"] = note.NotesField(trackPointCurrent.Notes).AsNoteString()
	}
	return p
}

func FeatureToTrack(f geojson.Feature) (TrackPoint, error) {
	var err error
	tp := TrackPoint{}

	p, ok := f.Geometry.(orb.Point)
	if !ok {
		return tp, errors.New("not a point")
	}
	tp.Lng = p.Lon()
	tp.Lat = p.Lat()

	if v, ok := f.Properties["UUID"]; ok {
		tp.Uuid = v.(string)
	}
	if v, ok := f.Properties["Name"]; ok {
		tp.Name = v.(string)
	}
	if v, ok := f.Properties["Time"]; ok {
		tp.Time, err = time.Parse(time.RFC3339, v.(string))
		if err != nil {
			return tp, err
		}
	}
	if v, ok := f.Properties["Version"]; ok {
		tp.Version = v.(string)
	}
	if v, ok := f.Properties["Speed"]; ok {
		tp.Speed = v.(float64)
	}
	if v, ok := f.Properties["Elevation"]; ok {
		tp.Elevation = v.(float64)
	}
	if v, ok := f.Properties["Heading"]; ok {
		tp.Heading = v.(float64)
	}
	if v, ok := f.Properties["Accuracy"]; ok {
		tp.Accuracy = v.(float64)
	}
	if v, ok := f.Properties["HeartRate"]; ok {
		tp.HeartRate = v.(float64)
	}
	anyNotes := false
	notes := NoteStructured{}
	if v, ok := f.Properties["Activity"]; ok {
		notes.Activity = v.(string)
		anyNotes = true
	}
	if v, ok := f.Properties["Pressure"]; ok {
		notes.Pressure = v.(float64)
		anyNotes = true
	}
	if v, ok := f.Properties["imgS3"]; ok {
		notes.ImgS3 = v.(string)
		anyNotes = true
	}
	if anyNotes {
		tp.Notes = notes.MustAsString()
	}
	return tp, nil
}
