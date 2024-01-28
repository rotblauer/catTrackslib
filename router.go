package catTrackslib

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

// https://github.com/gorilla/mux#middleware

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

func contentTypeMiddlewareFor(contentType string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Do stuff here
			w.Header().Set("Content-Type", contentType)
			// Call the next handler, which can be another middleware in the chain, or the final handler.
			next.ServeHTTP(w, r)
		})
	}
}

// // Define our struct
// type authenticationMiddleware struct {
// 	tokenUsers map[string]string
// }
//
// // Middleware function, which will be called for each request
// func (amw *authenticationMiddleware) Middleware(next http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		token := r.Header.Get("X-Session-Token")
//
// 		if user, found := amw.tokenUsers[token]; found {
// 			// We found the token in our map
// 			log.Printf("Authenticated user %s\n", user)
// 			// Pass down the request to the next middleware (or final handler)
// 			next.ServeHTTP(w, r)
// 		} else {
// 			// Write an error and stop the handler chain
// 			http.Error(w, "Forbidden", http.StatusForbidden)
// 		}
// 	})
// }

func tokenAuthenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validToken := os.Getenv("COTOKEN")
		if validToken == "" {
			log.Printf("No COTOKEN set, allowing all requests")
			next.ServeHTTP(w, r)
			return
		}

		token := r.Header.Get("AuthorizationOfCats")
		if token == "" {
			// Header token not set. Check alternate protocol, which is using a query param with the name api_token.
			// eg. catonmap.info:3001/populate/?api_token=asdfasdfb
			r.ParseForm()
			token = r.FormValue("api_token")
		}

		// Enforce token validation.
		if token != validToken {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		// Pass down the request to the next middleware (or final handler)
		next.ServeHTTP(w, r)
	})
}

func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	router.Use(loggingMiddleware)

	apiRoutes := router.NewRoute().Subrouter()
	apiRoutes.Use(corsMiddleware)

	jsonMiddleware := contentTypeMiddlewareFor("application/json")
	apiRoutes.Use(jsonMiddleware)

	authenticatedAPIRoutes := apiRoutes.NewRoute().Subrouter()
	authenticatedAPIRoutes.Use(tokenAuthenticationMiddleware)

	authenticatedAPIRoutes.Methods(http.MethodPost).Path("/populate/").HandlerFunc(populatePoints)
	authenticatedAPIRoutes.Methods(http.MethodPost).Path("/populate").HandlerFunc(populatePoints)
	apiRoutes.Methods(http.MethodGet).Path("/lastknown").HandlerFunc(getLastKnown)
	apiRoutes.Methods(http.MethodGet).Path("/catsnaps").HandlerFunc(handleGetCatSnaps)

	// File server merveres
	ass := http.StripPrefix("/ass/", http.FileServer(http.Dir("./ass/")))
	router.PathPrefix("/ass/").Handler(ass)

	bower := http.StripPrefix("/bower_components/", http.FileServer(http.Dir("./bower_components/")))
	router.PathPrefix("/bower_components/").Handler(bower)

	return router
}
