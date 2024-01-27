package catTrackslib

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)

	middleWarePermissiveCORS := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")

	})

	router.Methods(http.MethodPost).Path("/populate/").HandlerFunc(populatePoints).
		HandlerFunc(middleWarePermissiveCORS)
	router.Methods(http.MethodGet).Path("/lastknown").HandlerFunc(getLastKnown).
		HandlerFunc(middleWarePermissiveCORS)
	router.Methods(http.MethodGet).Path("/catsnaps").HandlerFunc(handleGetCatSnaps).
		HandlerFunc(middleWarePermissiveCORS)

	// File server merveres
	ass := http.StripPrefix("/ass/", http.FileServer(http.Dir("./ass/")))
	router.PathPrefix("/ass/").Handler(ass)

	bower := http.StripPrefix("/bower_components/", http.FileServer(http.Dir("./bower_components/")))
	router.PathPrefix("/bower_components/").Handler(bower)

	return router
}
