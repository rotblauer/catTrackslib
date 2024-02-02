package catTrackslib

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/schema"
)

// // the html stuff of this thing
// var templates = func() *template.Template {
// 	// p := path.Join(os.Getenv("GOPATH"), "src", "github.com", "rotblauer", "catTracks", "templates")
// 	// if _, err := os.Stat(p); err != nil {
// 	p := "templates"
// 	// }
// 	p = path.Join(p, "*.html")
// 	// p := path.Join(os.Getenv("GOPATH"), "src", "github.com", "rotblauer", "catTracks", "templates", "*.html")
// 	// p := path.Join(os.Getenv("GOPATH"), "src", "github.com", "rotblauer", "catTracks", "templates", "*.html")
// 	return template.Must(template.ParseGlob(p))
// }()

// W// elcome, loads and servers all (currently) data pointers
// func getIndexTemplate(w http.ResponseWriter, r *http.Request) {
// 	templates.ExecuteTemplate(w, "base", nil)
// }
// func getRaceTemplate(w http.ResponseWriter, r *http.Request) {
// 	templates.ExecuteTemplate(w, "race", nil)
// }
// func getMapTemplate(w http.ResponseWriter, r *http.Request) {
// 	templates.ExecuteTemplate(w, "map", nil)
// }
// func getLeafTemplate(w http.ResponseWriter, r *http.Request) {
// 	templates.ExecuteTemplate(w, "leaf", nil)
// }

func socket(w http.ResponseWriter, r *http.Request) {
	// see ./socket.go
	GetMelody().HandleRequest(w, r)
}

func getRaceJSON(w http.ResponseWriter, r *http.Request) {
	var e error

	var renderer = make(map[string]interface{})
	var spans = map[string]int{
		"today": 1,
		"week":  7,
		"all":   10,
	}

	for span, spanVal := range spans {
		renderer[span], e = buildTimePeriodStats(spanVal)
		if e != nil {
			fmt.Println(e)
			http.Error(w, e.Error(), http.StatusInternalServerError)
		}
	}

	buf, e := json.Marshal(renderer)
	if e != nil {
		fmt.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
	}
	w.Write(buf)
}

func getPointsJSON(w http.ResponseWriter, r *http.Request) {
	query := parseQuery(r, w)

	data, eq := getData(query)
	if eq != nil {
		http.Error(w, eq.Error(), http.StatusInternalServerError)
	}
	fmt.Println("Receive ajax get data string ")
	w.Write(data)
}

var iftttWebhoook = "https://maker.ifttt.com/trigger/any_cat_visit/with/key/" + os.Getenv("IFTTT_WEBHOOK_TOKEN")

type IftttBodyCatVisit struct {
	Value1 string `json:"value1"`
	Value2 string `json:"value2"`
	Value3 int    `json:"value3"`
}

// func uploadCSV(w http.ResponseWriter, r *http.Request) {
// 	r.ParseMultipartForm(32 << 30)
// 	file, _, err := r.FormFile("uploadfile")
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	defer file.Close()
//
// 	lines, err := csv.NewReader(file).ReadAll()
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
//
// 	for _, line := range lines {
// 		var tp *trackPoint.TrackPoint
//
// 		tp.Name = line[0]
//
// 		if tp.Time, err = time.Parse(time.UnixDate, line[1]); err != nil {
// 			http.Error(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
// 		if tp.Lat, err = strconv.ParseFloat(line[2], 64); err != nil {
// 			http.Error(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
// 		if tp.Lng, err = strconv.ParseFloat(line[3], 64); err != nil {
// 			http.Error(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
//
// 		errS := storePoint(tp)
// 		if errS != nil {
// 			http.Error(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
//
// 	}
//
// 	http.Redirect(w, r, "/", 302) // the 300
//
// }

func getMetaData(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	b, e := getmetadata()
	if e != nil {
		log.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
	}
	fmt.Println("Got metadata:", len(b), "bytes")
	w.Write(b)
}

var decoder = schema.NewDecoder()

// returns response type image
func handleGetGoogleNearbyPhotos(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "invalid form: "+err.Error(), http.StatusBadRequest)
	}
	var qf QueryFilterGoogleNearbyPhotos
	err = decoder.Decode(&qf, r.Form) // note using r.Form, not r.PostForm
	if err != nil {
		http.Error(w, "err decoding request: "+err.Error(), http.StatusBadRequest)
	}

	b, e := getGoogleNearbyPhotos(qf)
	if e != nil {
		log.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
	}
	fmt.Println("Got googlenearby photos:", len(b), "bytes")
	w.Write(b)
}

func handleGetPlaces(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// parse params
	// NOTE:
	// func (r *Request) ParseForm() error
	// ParseForm populates r.Form and r.PostForm.
	//
	// For all requests, ParseForm parses the raw query from the URL and
	// updates r.Form.

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "invalid form: "+err.Error(), http.StatusBadRequest)
	}
	var qf QueryFilterPlaces
	err = decoder.Decode(&qf, r.Form) // note using r.Form, not r.PostForm
	if err != nil {
		http.Error(w, "err decoding request: "+err.Error(), http.StatusBadRequest)
	}

	b, e := getPlaces(qf)
	if e != nil {
		log.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
	}
	fmt.Println("Got places:", len(b), "bytes")
	w.Write(b)
}

func handleGetPlaces2(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")

	// parse params
	// NOTE:
	// func (r *Request) ParseForm() error
	// ParseForm populates r.Form and r.PostForm.
	//
	// For all requests, ParseForm parses the raw query from the URL and
	// updates r.Form.

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "invalid form: "+err.Error(), http.StatusBadRequest)
	}
	var qf QueryFilterPlaces
	err = decoder.Decode(&qf, r.Form) // note using r.Form, not r.PostForm
	if err != nil {
		http.Error(w, "err decoding request: "+err.Error(), http.StatusBadRequest)
	}

	b, e := getPlaces2(qf)
	if e != nil {
		log.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
	}
	fmt.Println("Got places:", len(b), "bytes")
	w.Write(b)
}
