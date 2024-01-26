package catTrackslib

import "net/http"

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

var routes = Routes{
	Route{
		"PointPopulator",
		"POST",
		"/populate/",
		populatePoints,
	},
	Route{
		"CatsLastKnown",
		"GET",
		"/lastknown",
		getLastKnown,
	},
	Route{
		"GetCatSnaps",
		"GET",
		"/catsnaps",
		handleGetCatSnaps,
	},
	// Route{
	// 	"Index",
	// 	"GET",
	// 	"/",
	// 	getIndexTemplate,
	// },
	// Route{
	// 	"UploadCSV",
	// 	"POST",
	// 	"/upload",
	// 	uploadCSV,
	// },
	// Route{
	// 	"getPointsJSON",
	// 	"GET",
	// 	"/api/data/{version}",
	// 	getPointsJSON,
	// },
	// Route{
	// 	"WS",
	// 	"GET",
	// 	"/api/ws",
	// 	socket,
	// },
	// Route{
	// 	"Map",
	// 	"GET",
	// 	"/map",
	// 	getMapTemplate,
	// },
	// Route{
	// 	"Leaf",
	// 	"GET",
	// 	"/leaf",
	// 	getLeafTemplate,
	// },
	// Route{
	// 	"Race",
	// 	"GET",
	// 	"/race",
	// 	getRaceTemplate,
	// },
	// Route{
	// 	"RaceJSON",
	// 	"GET",
	// 	"/api/race",
	// 	getRaceJSON,
	// },
	// Route{
	// 	"StatsJSON",
	// 	"GET",
	// 	"/stats",
	// 	getStatsJSON,
	// },
	// Route{
	// 	"Metadata",
	// 	"GET",
	// 	"/metadata",
	// 	getMetaData,
	// },
	// Route{
	// 	"GetVisits",
	// 	"GET",
	// 	"/visits",
	// 	handleGetPlaces,
	// },
	// Route{
	// 	"GetVisits2",
	// 	"GET",
	// 	"/visits2",
	// 	handleGetPlaces2,
	// },
	// Route{
	// 	"GetVisitPhotos",
	// 	"GET",
	// 	"/googleNearbyPhotos",
	// 	handleGetGoogleNearbyPhotos,
	// },

}
