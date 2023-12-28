package catTrackslib

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rotblauer/trackpoints/trackPoint"
)

func mustParseTime(format, str string) time.Time {
	out, _ := time.Parse(format, str)
	return out
}

func TestTrackToFeature2(t *testing.T) {
	mockTP := &trackPoint.TrackPoint{
		Uuid:      "05C63745-BFA3-4DE3-AF2F-CDE2173C0E11",
		PushToken: "70fc05b3626303402b46b742a4d9aa7528472de6741ee8ccd6edbb35585702d7",
		Version:   "V.customizableCatTrackHat",
		ID:        1703258727017000000,
		Name:      "Rye13",
		Lat:       44.98931121826172,
		Lng:       -93.25544738769531,
		Accuracy:  15.054978370666504,

		VAccuracy:  0,
		Elevation:  323.27960205078125,
		Speed:      0,
		Tilt:       0,
		Heading:    293.32611083984375,
		HeartRate:  0,
		Time:       mustParseTime(time.RFC3339, "2023-12-22T15:42:05.018Z"),
		Floor:      0,
		Notes:      "{\"floorsAscended\":56,\"customNote\":\"\",\"heartRateS\":\"\",\"currentTripStart\":\"2023-12-16T15:25:40.335Z\",\"floorsDescended\":40,\"averageActivePace\":0.53959662255944008,\"networkInfo\":\"{}\",\"numberOfSteps\":65066,\"visit\":\"{\\\"validVisit\\\":false}\",\"relativeAltitude\":-16.205322265625,\"currentCadence\":1.851201057434082,\"heartRateRawS\":\"\",\"batteryStatus\":\"{\\\"level\\\":0.94999998807907104,\\\"status\\\":\\\"unplugged\\\"}\",\"activity\":\"Stationary\",\"currentPace\":0.69526106119155884,\"imgb64\":\"\",\"pressure\":97.975234985351562,\"distance\":69266.793879101984}",
		COVerified: true,
		RemoteAddr: "",
	}

	feature := TrackToFeature(mockTP)
	j, _ := json.MarshalIndent(feature, "", "  ")
	t.Log(string(j))

	feature2 := TrackToFeature2(mockTP)
	j2, _ := json.MarshalIndent(feature2, "", "  ")
	t.Log(string(j2))

}
