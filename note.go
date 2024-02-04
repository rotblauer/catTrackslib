package catTrackslib

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"
	// // Package image/jpeg is not used explicitly in the code below,
	// // but is imported for its initialization side-effect, which allows
	// _ "image/gif"
	// // image.Decode to understand JPEG formatted images. Uncomment these
	// _ "image/jpeg"
	// // two lines to also understand GIF and PNG images:
	// _ "image/png"
)

// private func objectifyNote(n: Note) -> NSMutableDictionary? {
// 	let dict = NSMutableDictionary()
// 	dict.setValue(n.activity.rawValue, forKey: "activity");  //set all your values..
// 	dict.setValue(n.numberOfSteps, forKey: "numberOfSteps");
// 	dict.setValue(n.averageActivePace, forKey: "averageActivePace");
// 	dict.setValue(n.currentPace, forKey: "currentPace");
// 	dict.setValue(n.currentCadence, forKey: "currentCadence");
// 	dict.setValue(n.distance, forKey: "distance");
// 	dict.setValue(n.customNote, forKey: "customNote");
// 	dict.setValue(n.floorsAscended, forKey: "floorsAscended");
// 	dict.setValue(n.floorsDescended, forKey: "floorsDescended");
// 	dict.setValue(n.currentTripStart.iso8601, forKey: "currentTripStart");
// 	dict.setValue(n.relativeAltitude, forKey: "relativeAltitude");
// 	dict.setValue(n.pressure, forKey: "pressure");
// 	dict.setValue(getStringVisit(v:n.currentVisit), forKey: "visit");

// 	return dict
// }

// type Note struct {
// 	Activity          string      `json:"activity"`
// 	NumberOfSteps     int         `json:"numberOfSteps"`
// 	Pressure          float64     `json:"pressure"`
// 	AverageActivePace float64     `json:"averageActivePace"`
// 	CustomNote        interface{} `json:"customNote"`
// }

var ErrNilNote = errors.New("nil note")

type NotesField []byte

// // MarshalJSON returns *m as the JSON encoding of m.
// func (m *NotesField) MarshalJSON() ([]byte, error) {
// 	return []byte(*m), nil
// }

// // UnmarshalJSON sets *m to a copy of data.
// func (m *NotesField) UnmarshalJSON(data []byte) error {
// 	if m == nil {
// 		return errors.New("RawString: UnmarshalJSON on nil pointer")
// 	}
// 	*m += RawString(data)
// 	return nil
// }

type NoteString string

func (nf NotesField) AsNoteString() string {
	if nf == nil {
		return ""
	}
	return string(nf)
}

type NoteFingerprint struct {
	fingerprintMD5  string `json:"fingerprintHashMD5"`
	fingerprint_MD5 string `json:"fingerprintHash"`
}

func (nf NotesField) AsFingerprint() (fing NoteFingerprint, err error) {
	if nf == nil {
		err = ErrNilNote
		return
	}
	err = json.Unmarshal(nf, &fing)
	return
}

func (fp NoteFingerprint) Value() []byte {
	if fp.fingerprint_MD5 == "" {
		return []byte(fp.fingerprintMD5)
	}
	return []byte(fp.fingerprint_MD5)
}

type NoteStructured struct {
	Activity           string              `json:"activity"`
	ActivityConfidence *int                `json:"activity_confidence"`
	NumberOfSteps      int                 `json:"numberOfSteps"`
	AverageActivePace  float64             `json:"averageActivePace"`
	CurrentPace        float64             `json:"currentPace"`
	CurrentCadence     float64             `json:"currentCadence"`
	Distance           float64             `json:"distance"`
	CustomNote         string              `json:"customNote"` // FIXME: string or float64?
	FloorsAscended     int                 `json:"floorsAscended"`
	FloorsDescended    int                 `json:"floorsDescended"`
	CurrentTripStart   time.Time           `json:"currentTripStart"`
	Pressure           float64             `json:"pressure"`
	Visit              VisitString         `json:"visit"`
	HeartRateS         string              `json:"heartRateS"`
	HeartRateRawS      string              `json:"heartRateRawS"`
	BatteryStatus      BatteryStatusString `json:"batteryStatus"`
	NetworkInfo        NetworkInfoString   `json:"networkInfo"`
	ImgB64             string              `json:"imgb64"`
	ImgS3              string              `json:"imgS3"`
	Lightmeter         float64             `json:"lightmeter,omitempty"`
	AmbientTemp        float64             `json:"ambient_temp,omitempty"`
	Humidity           float64             `json:"humidity,omitempty"`
	Accelerometer
	UserAccelerometer
	Gyroscope
}

type Accelerometer struct {
	X *float64 `json:"accelerometer_x,omitempty"`
	Y *float64 `json:"accelerometer_y,omitempty"`
	Z *float64 `json:"accelerometer_z,omitempty"`
}
type UserAccelerometer struct {
	X *float64 `json:"user_accelerometer_x,omitempty"`
	Y *float64 `json:"user_accelerometer_y,omitempty"`
	Z *float64 `json:"user_accelerometer_z,omitempty"`
}
type Gyroscope struct {
	X *float64 `json:"gyroscope_x,omitempty"`
	Y *float64 `json:"gyroscope_y,omitempty"`
	Z *float64 `json:"gyroscope_z,omitempty"`
}

type NetworkInfoString string
type BatteryStatusString string

func (ns NoteStructured) HasRawImage() bool {
	return ns.ImgB64 != ""
}

func (ns NoteStructured) HasS3Image() bool {
	return ns.ImgS3 != ""
}

type NetworkInfo struct {
	SSID     string `json:"ssid"`
	SSIDData string `json:"ssidData"`
	BSSID    string `json:"bssid"`
}

type BatteryStatus struct {
	Level  float64 `json:"level"`
	Status string  `json:"status"`
}

type VisitString string

func (vs VisitString) AsVisit() (v NoteVisit, err error) {
	err = json.Unmarshal([]byte(vs), &v)
	if err != nil {
		return
	}
	v.ArrivalTime, err = time.Parse(time.RFC3339Nano, v.ArrivalTimeString)
	if err != nil {
		return
	}
	v.DepartureTime, err = time.Parse(time.RFC3339Nano, v.DepartureTimeString)
	return
}

func (nf NotesField) AsNoteStructured() (ns NoteStructured, err error) {
	if nf == nil {
		err = ErrNilNote
		return
	}
	err = json.Unmarshal(nf, &ns)
	return
}

func (ns NoteStructured) MustAsString() string {
	b, err := json.Marshal(ns)
	if err != nil {
		log.Fatal(err) // FIXME yikes
	}
	return string(b)
}

func (ns NoteStructured) HeartRateI() float64 {
	if ns.HeartRateS == "" {
		return -1
	}
	f, err := strconv.ParseFloat(strings.Split(ns.HeartRateS, " ")[0], 64)
	if err != nil {
		return -2
	}
	return f
}

// // 25 Yeadon Ave, 25 Yeadon Ave, Charleston, SC  29407, United States @ <+32.78044829,-79.98285770> +\\\/- 100.00m, region CLCircularRegion (identifier:'<+32.78044828,-79.98285770> radius 141.76', center:<+32.78044828,-79.98285770>, radius:141.76m)
type PlaceString string

type Place struct {
	Identity string
	Address  string
	Lat      float64
	Lng      float64
	Acc      float64
	Radius   float64
}

func (ns NoteStructured) HasVisit() bool {
	v, err := ns.Visit.AsVisit()
	if err != nil {
		return false
	}
	// if v.ArrivalTime.IsZero() {
	// 	panic("zero arrivals")
	// }
	if v.ArrivalTime.IsZero() && v.DepartureTime.IsZero() {
		return false
	}
	return v.Place != ""
}

func (ns NoteStructured) HasValidVisit() bool {
	if !ns.HasVisit() {
		return false
	}
	v, _ := ns.Visit.AsVisit()
	return v.Valid
}

type NoteVisit struct {
	Uuid                string `json:"uuid"` // kind of optional
	Name                string `json:"name"` // kind of optional
	ArrivalTime         time.Time
	ArrivalTimeString   string `json:"arrivalDate"`
	DepartureTime       time.Time
	DepartureTimeString string      `json:"departureDate"`
	Place               PlaceString `json:"place"`
	PlaceParsed         Place
	Valid               bool `json:"validVisit"`
	ReportedTime        time.Time
	Duration            time.Duration
	// GoogleNearby        *gm.PlacesSearchResponse `json:"googleNearby,omitempty"`
	GoogleNearbyPhotos map[string]string `json:"googleNearbyPhotos,omitempty"` // photoreference:base64img
}

//	func (nv NoteVisit) GetDuration() time.Duration {
//		calend := nv.DepartureTime
//		// seen "departureDate\":\"4001-01-01T00:00:00.000Z\"}
//		if nv.DepartureTime.Year() == 4001 || nv.DepartureTime.After(time.Now().Add(24*365*time.Hour)) {
//			calend = time.Now()
//		}
//		return calend.Sub(nv.ArrivalTime)
//	}
//
// // map = photoreference:base64 encoded image
//
//	func (v NoteVisit) GoogleNearbyImagesQ() (map[string]string, error) {
//		if v.GoogleNearby == nil {
//			return nil, errors.New("must get visit nearbys first")
//		}
//
//		var err error
//		var ret = make(map[string]string)
//
//		// https://maps.googleapis.com/maps/api/place/photo
//		u, err := url.Parse("https://maps.googleapis.com/maps/api/place/photo")
//		if err != nil {
//			return ret, err
//		}
//		q := u.Query()
//		q.Set("maxwidth", "400")
//		q.Set("key", os.Getenv("GOOGLE_PLACES_API_KEY"))
//
//		for _, nb := range v.GoogleNearby.Results {
//			if len(nb.Photos) == 0 {
//				continue
//			}
//
//			// only FIRST PHOTO for now FIXME
//			ref := nb.Photos[0].PhotoReference
//			q.Set("photoreference", ref)
//
//			u.RawQuery = q.Encode()
//
//			res, err := http.Get(u.String())
//			if err != nil {
//				return ret, err
//			}
//
//			b, err := ioutil.ReadAll(res.Body)
//			if err != nil {
//				return ret, err
//			}
//			err = res.Body.Close()
//			if err != nil {
//				return ret, err
//			}
//
//			ret[ref] = base64.StdEncoding.EncodeToString(b)
//		}
//		return ret, err
//	}
//
// func (visit NoteVisit) GoogleNearbyQ() (res *gm.PlacesSearchResponse, err error) {
//
//		// ios radius for visit is cautious, and google is prolific. thus, optimism. high number = small google radius param
//		// raw radius numbers are typically 140 or 70 meters
//		var divideRadius = 2.0
//
//		res = &gm.PlacesSearchResponse{}
//		u, err := url.Parse("https://maps.googleapis.com/maps/api/place/nearbysearch/json")
//		if err != nil {
//			log.Println("could not parse google url", err)
//			return res, err
//		}
//		q := u.Query()
//		q.Set("location", fmt.Sprintf("%.14f,%.14f", visit.PlaceParsed.Lat, visit.PlaceParsed.Lng))
//		var r float64
//		r = visit.PlaceParsed.Radius
//		if r == 0 {
//			r = visit.Place.GetRadius()
//		}
//		if r == 0 {
//			r = 50
//		}
//		r = r / divideRadius
//		q.Set("radius", fmt.Sprintf("%.2f", r))
//
//		q.Set("rankby", "prominence") // also distance, tho distance needs name= or type= or somethin
//
//		q.Set("key", os.Getenv("GOOGLE_PLACES_API_KEY"))
//
//		u.RawQuery = q.Encode()
//
//		// log.Println("query => ", u.String())
//
//		re, err := http.Get(u.String())
//		if err != nil {
//			log.Println("error google nearby http req", err)
//			return res, err
//		}
//
//		err = json.NewDecoder(re.Body).Decode(&res)
//		// b := []byte{}
//		// _, err = re.Body.Read(b)
//		// if err != nil {
//		// 	log.Println("could not read res body", err)
//		// 	return res, err
//		// }
//		// re.Body.Close()
//
//		// // unmarshal
//		// err = json.Unmarshal(b, &res)
//		return
//	}
//
//
// func (ps PlaceString) GetRadius() float64 {
// 	r := strings.Split(string(ps), "radius:")[1]
// 	// log.Println("r1", r)
//
// 	r = strings.Split(r, "m")[0]
// 	// log.Println("r2", r)
//
// 	rn, err := strconv.ParseFloat(r, 64)
// 	if err != nil {
// 		return 0
// 	}
// 	return rn
// }
//
// func (ps PlaceString) AsPlace() (p Place, err error) {
// 	// slices will panic if oob
// 	commas := strings.Split(string(ps), ",")
// 	p.Identity = commas[0]
// 	p.Address = strings.Split(string(ps), "@")[0] // TODO: remove 'identity' prefix?
//
// 	s1 := strings.Split(string(ps), "<")[1]
// 	s1 = strings.Split(s1, ">")[0]
// 	ll := strings.Split(s1, ",")
// 	lat := strings.TrimPrefix(ll[0], "+")
// 	lng := strings.TrimPrefix(ll[1], "+")
//
// 	p.Lat, err = strconv.ParseFloat(lat, 64)
// 	if err != nil {
// 		return
// 	}
// 	p.Lng, err = strconv.ParseFloat(lng, 64)
// 	if err != nil {
// 		return
// 	}
//
// 	p.Radius = ps.GetRadius()
//
// 	// TODO p.Acc, p.Radius
// 	return
// }
//
// func (ps PlaceString) MustAsPlace() Place {
// 	p, _ := ps.AsPlace()
// 	return p
// }
