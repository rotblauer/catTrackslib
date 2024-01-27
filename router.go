package catTrackslib

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)

	router.Methods("POST").Path("/populate/").HandlerFunc(populatePoints)
	router.Methods("GET").Path("/lastknown").HandlerFunc(getLastKnown)
	router.Methods("GET").Path("/catsnaps").HandlerFunc(handleGetCatSnaps)

	// File server merveres
	ass := http.StripPrefix("/ass/", http.FileServer(http.Dir("./ass/")))
	router.PathPrefix("/ass/").Handler(ass)

	bower := http.StripPrefix("/bower_components/", http.FileServer(http.Dir("./bower_components/")))
	router.PathPrefix("/bower_components/").Handler(bower)

	return router
}
