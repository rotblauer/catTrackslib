package catTrackslib

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do stuff here
		log.Println(r.RequestURI)
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do stuff here
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(w, r)
	})
}

func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	router.Use(loggingMiddleware)

	apiRoutes := router.NewRoute().Subrouter()
	apiRoutes.Use(corsMiddleware)

	apiRoutes.Methods(http.MethodPost).Path("/populate/").HandlerFunc(populatePoints)
	apiRoutes.Methods(http.MethodGet).Path("/lastknown").HandlerFunc(getLastKnown)
	apiRoutes.Methods(http.MethodGet).Path("/catsnaps").HandlerFunc(handleGetCatSnaps)

	// File server merveres
	ass := http.StripPrefix("/ass/", http.FileServer(http.Dir("./ass/")))
	router.PathPrefix("/ass/").Handler(ass)

	bower := http.StripPrefix("/bower_components/", http.FileServer(http.Dir("./bower_components/")))
	router.PathPrefix("/bower_components/").Handler(bower)

	return router
}
