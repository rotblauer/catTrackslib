package catTrackslib

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	note "github.com/rotblauer/catnotelib"
	"github.com/rotblauer/trackpoints/trackPoint"
	bolt "go.etcd.io/bbolt"
	gm "googlemaps.github.io/maps"
)

var punktlichTileDBPathRelHome = filepath.Join("punktlich.rotblauer.com", "tester.db")

// https://stackoverflow.com/a/31832326/4401322
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type Metadata struct {
	KeyN               int
	KeyNUpdated        time.Time
	LastUpdatedAt      time.Time
	LastUpdatedBy      string
	LastUpdatedPointsN int
	TileDBLastUpdated  time.Time
}

type QueryFilterPlaces struct {
	Uuids []string `schema:"uuids"`
	Names []string `schema:"names"`
	// PushTokens []string `schema:"pushTokens"` // TODO

	// start,end x arrive,depart,report
	StartArrivalT   time.Time `schema:"startArrivalT"`
	EndArrivalT     time.Time `schema:"endArrivalT"`
	StartDepartureT time.Time `schema:"startDepartureT"`
	EndDepartureT   time.Time `schema:"endDepartureT"`
	StartReportedT  time.Time `schema:"startReportedT"`
	EndReportedT    time.Time `schema:"endReportedT"`

	ReverseChrono bool `schema:"rc"` // when true, oldest first; default to newest first

	// paginatables
	StartIndex int `schema:"startI"` // 0 indexed;
	EndIndex   int `schema:"endI"`   // diff end-start = per/pagination lim

	// for geo rect bounding, maybe
	LatMin *float64 `schema:"latmin"`
	LatMax *float64 `schema:"latmax"`
	LngMin *float64 `schema:"lngmin"`
	LngMax *float64 `schema:"lngmax"`

	BoundingBoxSW []float64 `schema:"bboxSW"`
	BoundingBoxNE []float64 `schema:"bboxNE"`

	IncludeStats bool `schema:"stats,omitempty"`

	GoogleNearby       bool `schema:"googleNearby"`
	GoogleNearbyPhotos bool `schema:"googleNearbyPhotos"`
}

type QueryFilterGoogleNearbyPhotos struct {
	PhotoReference string `schema:"photoreference"`
}

type VisitsResponse struct {
	Visits    []*note.NoteVisit `json:"visits"`
	Stats     bolt.BucketStats  `json"bucketStats,omitempty"`
	StatsTook time.Duration     `json:"statsTook,omitempty"` // how long took to get bucket stats (for 10mm++ points, long time)
	Scanned   int               `json:"scanned"`             // num visits checked before mtaching filters
	Matches   int               `json:"matches"`             // num visits matching before paging/index filters
}

// var DefaultQFP = QueryFilterPlaces{
// 	Names:    []string{},
// 	EndIndex: 30, // given zero values, reverse and StartIndex=0, this returns 30 most recent places
// 	LatMin:   math.MaxFloat64,
// 	LatMax:   math.MaxFloat64,
// 	LngMin:   math.MaxFloat64,
// 	LatMax:   math.MaxFloat64,
// }

// // ByTime implements Sort interface for NoteVisit
// type ByTime []note.NoteVisit

// func (bt ByTime) Len() int {
// 	return len(bt)
// }

// func (bt ByTime) Swap(i, j int) {
// 	bt[i], bt[j] = bt[j], bt[i]
// }

// // Less compares ARRIVALTIME. This might need to be expanded or differentiated.
// func (bt ByTime) Less(i, j int) bool {
// 	return bt[i].ArrivalTime.Before(bt[j].ArrivalTime)
// }

// decode base64 from db and return image
func getGoogleNearbyPhotos(qf QueryFilterGoogleNearbyPhotos) (out []byte, err error) {
	var data []byte
	var dne = errors.New("resource does not exist")
	err = GetDB("master").View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(googlefindnearbyphotos))
		data = b.Get([]byte(qf.PhotoReference))
		if data == nil {
			return dne
		}
		return nil
	})
	if err != nil {
		if err == dne {
			log.Println("err", err, "querying googlenearby photos api to fill gap")

			// https://maps.googleapis.com/maps/api/place/photo
			u, er := url.Parse("https://maps.googleapis.com/maps/api/place/photo")
			if er != nil {
				err = er
				log.Println("err", er)
				return out, err
			}
			q := u.Query()
			q.Set("maxwidth", "400")
			q.Set("key", os.Getenv("GOOGLE_PLACES_API_KEY"))

			q.Set("photoreference", qf.PhotoReference)
			log.Println("photoref", qf.PhotoReference)

			u.RawQuery = q.Encode()

			res, er := http.Get(u.String())
			if er != nil {
				log.Println("err", er)
				err = er
				return out, err
			}

			b, er := ioutil.ReadAll(res.Body)
			if er != nil {
				log.Println("err", er)
				out, err = b, er
				return out, er
			}
			er = res.Body.Close()
			if er != nil {
				log.Println("err", er)
				return out, er
			}

			log.Println("res google photo len=", len(b))

			b64s := base64.StdEncoding.EncodeToString(b)
			data = []byte(b64s)
			log.Println("b64:", b64s)

			if er := GetDB("master").Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(googlefindnearbyphotos))
				err := b.Put([]byte(qf.PhotoReference), data)
				return err
			}); er != nil {
				err = er
				log.Println("err", er)
				return out, err
			} else {
				log.Println("saved google photo OK", qf.PhotoReference, "slen", len(b64s))
			}
		} else {
			return data, err
		}
	}

	reader := base64.NewDecoder(base64.StdEncoding, bytes.NewBuffer(data))
	// config, format, err := image.DecodeConfig(reader)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("Width:", config.Width, "Height:", config.Height, "Format:", format)
	m, s, err := image.Decode(reader)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(out)
	if strings.Contains(s, "jpeg") {
		err = jpeg.Encode(buf, m, nil)
	} else {
		err = png.Encode(buf, m)
	}
	if err != nil {
		return out, err
	}
	out, err = ioutil.ReadAll(buf)
	return
}

// btw places are actually visits. fucked that one up.
func getPlaces2(qf QueryFilterPlaces) (out []byte, err error) {
	// TODO
	// - wire to router with query params
	// - filter during key iter
	// - sortable interface places
	// - places to json, new type in Note

	log.Println("handling visits (2) q:", spew.Sdump(qf))

	var res = VisitsResponse{}
	var visits = []*note.NoteVisit{}
	var scannedN, matchingN int // nice convenience returnables for query stats, eg. matched 4/14 visits, querier can know this

	filterVisQF := func(tx *bolt.Tx, k []byte, qf QueryFilterPlaces, nv *note.NoteVisit) bool {
		// filter: uuids using key[9:] (since buildTrackpointKey appends 8-byte timenano + uuid...)
		// and vis uses "foreign key index" from trackPoint master db
		if len(qf.Uuids) > 0 {
			var ok bool
			for _, u := range qf.Uuids {
				if bytes.Equal(k[9:], []byte(u)) {
					ok = true
					break
				}
			}
			if !ok {
				return false // ==ok
			}
		}
		if nv.Uuid == "" {
			nv.Uuid = string(k[9:])
		}

		// filter: names
		if len(qf.Names) > 0 || nv.Name == "" {
			// fuck... gotta x-reference tp to check cat names
			var tp = &trackPoint.TrackPoint{}

			bt := tx.Bucket([]byte(trackKey))
			tpv := bt.Get(k)
			if tpv == nil {
				log.Println("no trackpoint stored for visit:", nv)
				return false
			}
			err = json.Unmarshal(tpv, tp)
			if err != nil {
				log.Println("err unmarshalling tp for visit query:", err)
				return false
			}
			nv.Name = tp.Name
		}
		if len(qf.Names) > 0 {
			var ok bool
			for _, n := range qf.Names {
				if n == nv.Name {
					ok = true
					break
				}
			}
			if !ok {
				// doesn't match any of the given whitelisted names
				return false
			}
		}

		// filter: start/endT
		if !qf.StartArrivalT.IsZero() {
			if nv.ArrivalTime.Before(qf.StartArrivalT) {
				return false
			}
		}
		if !qf.StartDepartureT.IsZero() {
			if nv.DepartureTime.Before(qf.StartDepartureT) {
				return false
			}
		}
		if !qf.EndArrivalT.IsZero() {
			if nv.ArrivalTime.After(qf.EndArrivalT) {
				return false
			}
		}
		if !qf.EndDepartureT.IsZero() {
			if nv.DepartureTime.After(qf.EndDepartureT) {
				return false
			}
		}
		return true
	}

	getVisitGoogleInfo := func(tx *bolt.Tx, k []byte, nv *note.NoteVisit) {
		// lookup local google
		if qf.GoogleNearby {
			pg := tx.Bucket([]byte(googlefindnearby))
			gr := pg.Get(k)
			if gr != nil {
				r := &gm.PlacesSearchResponse{}
				if err := json.Unmarshal(gr, &r); err == nil {
					// log.Println("found existing gn data", spew.Sdump(r))
					nv.GoogleNearby = r
				}
			} else {
				// hasn't BEEN googled yet, google it and SAVE an ok response
				log.Println("no existing gnb data, querying...")
				r, err := nv.GoogleNearbyQ()
				if err == nil && r != nil {
					log.Println("googleNearby OK")
					b, err := json.Marshal(r)
					if err == nil {
						if err := pg.Put(k, b); err != nil {
							log.Println("could not write gnb to db", err)
						} else {
							log.Println("saved gnb OK")
						}
					}
					nv.GoogleNearby = r
				} else {
					log.Println("could not googlenearby", err, "visit", nv)
				}
			}

			if qf.GoogleNearbyPhotos {
				var gnp = make(map[string]string)

				pp := tx.Bucket([]byte(googlefindnearbyphotos))

				for _, r := range nv.GoogleNearby.Results {
					if len(r.Photos) == 0 {
						continue
					}
					ref := r.Photos[0].PhotoReference
					if b64 := pp.Get([]byte(ref)); b64 != nil {
						gnp[ref] = string(b64)
					}
				}

				// if we got NOTHING, then lookup. this, this is mostly ugly ROLLOUT feature
				if len(gnp) == 0 {
					// go grab em
					// google ref photos
					placePhotos, err := nv.GoogleNearbyImagesQ()
					if err != nil {
						log.Println("could not query visit photos", err)
					} else {
						for ref, b64 := range placePhotos {
							if err := pp.Put([]byte(ref), []byte(b64)); err != nil {
								log.Println("err storing photoref:b64", err)
							}
						}
						nv.GoogleNearbyPhotos = placePhotos
					}
				} else {
					nv.GoogleNearbyPhotos = gnp
				}
			}
		}
	}

	// FIXME: only Update r b/c case we want to go grab the google nearby for a visit. not necessary otherwise
	err = GetDB("master").Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(placesKey))
		if b == nil {
			return fmt.Errorf("no places bolt bucket exists")
		}
		c := b.Cursor()

		if qf.IncludeStats {
			t1 := time.Now()
			res.Stats = b.Stats()
			res.StatsTook = time.Since(t1)
		}

		// unclosed forloop conditions
		k, vis := c.First()
		condit := func() bool {
			return k != nil
		}

		if qf.BoundingBoxSW != nil && qf.BoundingBoxNE != nil {
			if len(qf.BoundingBoxSW) != 2 || len(qf.BoundingBoxNE) != 2 {
				return fmt.Errorf("invalid bounding box params sw=%v ne=%v", qf.BoundingBoxSW, qf.BoundingBoxNE)
			}
			b = tx.Bucket([]byte(placesByCoord))
			if b == nil {
				return fmt.Errorf("no placesByCoord bolt bucket exists")
			}
			c = b.Cursor()

			min := Float64bytesBig(qf.BoundingBoxSW[0] + 90)
			min = append(min, Float64bytesBig(qf.BoundingBoxSW[1]+180)...)
			if !qf.StartReportedT.IsZero() {
				min = append(min, i64tob(qf.StartReportedT.UnixNano())...)
			} else {
				min = append(min, i64tob(0)...)
			}

			max := Float64bytesBig(qf.BoundingBoxNE[0] + 90)
			max = append(max, Float64bytesBig(qf.BoundingBoxNE[1]+180)...)
			if !qf.EndReportedT.IsZero() {
				max = append(max, i64tob(qf.EndReportedT.UnixNano())...)
			} else {
				max = append(max, i64tob(time.Now().UnixNano())...)
			}

			k, vis = c.Seek(min)
			condit = func() bool {
				return k != nil && bytes.Compare(k, max) <= 0
			}
		} else {
			if !qf.StartReportedT.IsZero() {
				// use fancy seekers/condits if reported time query parameter comes thru
				// when there's a lot of visits, this might be moar valuable
				// Note that reported time is trackpoint time, and that's the key we range over. Same key as tp.
				k, vis = c.Seek(i64tob(qf.StartReportedT.UnixNano()))
			}
			if !qf.EndReportedT.IsZero() {
				condit = func() bool {
					// i64tob uses big endian with 8 bytes
					return k != nil && bytes.Compare(k[:8], i64tob(qf.EndReportedT.UnixNano())) <= 0
				}
			}
		}

		for ; condit(); k, vis = c.Next() {

			scannedN++

			var nv = &note.NoteVisit{}

			err := json.Unmarshal(vis, nv)
			if err != nil {
				log.Println("error unmarshalling visit for query:", err)
				continue
			}

			match := filterVisQF(tx, k, qf, nv)
			if match {
				matchingN++
				getVisitGoogleInfo(tx, k, nv)
				visits = append(visits, nv)
			}
		}
		return nil
	})

	// filter: handle reverse Chrono
	if qf.ReverseChrono {
		// sort with custom Less function in closure
		sort.Slice(visits, func(i, j int) bool {
			return visits[i].ArrivalTime.Before(visits[j].ArrivalTime)
		})
	} else {
		sort.Slice(visits, func(i, j int) bool {
			return visits[i].ArrivalTime.After(visits[j].ArrivalTime)
		})
	}

	// filter: paginate with indexes, limited oob's
	// FIXME this might not even be right, just tryna avoid OoB app killers (we could theor allow negs, with fance reversing wrapping, but tldd)
	if qf.EndIndex == 0 || qf.EndIndex > len(visits) || qf.EndIndex < 0 {
		qf.EndIndex = len(visits)
	}
	if qf.StartIndex > len(visits) {
		qf.StartIndex = len(visits)
	}
	if qf.StartIndex < 0 {
		qf.StartIndex = 0
	}

	res.Visits = visits[qf.StartIndex:qf.EndIndex]
	res.Matches = matchingN
	res.Scanned = scannedN

	out, err = json.Marshal(res)

	return
}

// btw places are actually visits. fucked that one up.
func getPlaces(qf QueryFilterPlaces) (out []byte, err error) {
	// TODO
	// - wire to router with query params
	// - filter during key iter
	// - sortable interface places
	// - places to json, new type in Note

	log.Println("handling visits q:", spew.Sdump(qf))

	var res = VisitsResponse{}
	var visits = []*note.NoteVisit{}
	var scannedN, matchingN int // nice convenience returnables for query stats, eg. matched 4/14 visits, querier can know this

	// FIXME: only Update r b/c case we want to go grab the google nearby for a visit. not necessary otherwise
	err = GetDB("master").Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(placesKey))

		if qf.IncludeStats {
			t1 := time.Now()
			res.Stats = b.Stats()
			res.StatsTook = time.Since(t1)
		}

		if b == nil {
			return fmt.Errorf("no places bolt bucket exists")
		}
		c := b.Cursor()

		// unclosed forloop conditions
		k, vis := c.First()
		condit := func() bool {
			return k != nil
		}

		// use fancy seekers/condits if reported time query parameter comes thru
		// when there's a lot of visits, this might be moar valuable
		// Note that reported time is trackpoint time, and that's the key we range over. Same key as tp.
		if !qf.StartReportedT.IsZero() {
			k, vis = c.Seek(i64tob(qf.StartReportedT.UnixNano()))
		}
		if !qf.EndReportedT.IsZero() {
			condit = func() bool {
				// i64tob uses big endian with 8 bytes
				return k != nil && bytes.Compare(k[:8], i64tob(qf.EndReportedT.UnixNano())) <= 0
			}
		}

	ITERATOR:
		for ; condit(); k, vis = c.Next() {

			scannedN++

			var nv = &note.NoteVisit{}

			err := json.Unmarshal(vis, nv)
			if err != nil {
				log.Println("error unmarshalling visit for query:", err)
				continue ITERATOR
			}

			// filter: uuids using key[9:] (since buildTrackpointKey appends 8-byte timenano + uuid...)
			// and vis uses "foreign key index" from trackPoint master db
			if len(qf.Uuids) > 0 {
				var ok bool
				for _, u := range qf.Uuids {
					if bytes.Equal(k[9:], []byte(u)) {
						ok = true
						break
					}
				}
				if !ok {
					continue ITERATOR
				}
			}
			if nv.Uuid == "" {
				nv.Uuid = string(k[9:])
			}

			// filter: names
			if len(qf.Names) > 0 || nv.Name == "" {
				// fuck... gotta x-reference tp to check cat names
				var tp = &trackPoint.TrackPoint{}

				bt := tx.Bucket([]byte(trackKey))
				tpv := bt.Get(k)
				if tpv == nil {
					log.Println("no trackpoint stored for visit:", nv)
					continue
				}
				err = json.Unmarshal(tpv, tp)
				if err != nil {
					log.Println("err unmarshalling tp for visit query:", err)
					continue
				}
				nv.Name = tp.Name
			}
			if len(qf.Names) > 0 {
				var ok bool
				for _, n := range qf.Names {
					if n == nv.Name {
						ok = true
						break
					}
				}
				if !ok {
					// doesn't match any of the given whitelisted names
					continue ITERATOR
				}
			}

			// filter: start/endT
			if !qf.StartArrivalT.IsZero() {
				if nv.ArrivalTime.Before(qf.StartArrivalT) {
					continue ITERATOR
				}
			}
			if !qf.StartDepartureT.IsZero() {
				if nv.DepartureTime.Before(qf.StartDepartureT) {
					continue ITERATOR
				}
			}
			if !qf.EndArrivalT.IsZero() {
				if nv.ArrivalTime.After(qf.EndArrivalT) {
					continue ITERATOR
				}
			}
			if !qf.EndDepartureT.IsZero() {
				if nv.DepartureTime.After(qf.EndDepartureT) {
					continue ITERATOR
				}
			}

			// filter: lat,lng x min,max
			if qf.LatMin != nil {
				if nv.PlaceParsed.Lat < *qf.LatMin {
					continue ITERATOR
				}
			}
			if qf.LatMax != nil {
				if nv.PlaceParsed.Lat > *qf.LatMax {
					continue ITERATOR
				}
			}
			if qf.LngMax != nil {
				if nv.PlaceParsed.Lng > *qf.LngMax {
					continue ITERATOR
				}
			}
			if qf.LngMin != nil {
				if nv.PlaceParsed.Lng < *qf.LngMin {
					continue ITERATOR
				}
			}

			matchingN++

			// lookup local google
			if qf.GoogleNearby {
				pg := tx.Bucket([]byte(googlefindnearby))
				gr := pg.Get(k)
				if gr != nil {
					r := &gm.PlacesSearchResponse{}
					if err := json.Unmarshal(gr, &r); err == nil {
						// log.Println("found existing gn data", spew.Sdump(r))
						nv.GoogleNearby = r
					}
				} else {
					// hasn't BEEN googled yet, google it and SAVE an ok response
					log.Println("no existing gnb data, querying...")
					r, err := nv.GoogleNearbyQ()
					if err == nil && r != nil {
						log.Println("googleNearby OK")
						b, err := json.Marshal(r)
						if err == nil {
							if err := pg.Put(k, b); err != nil {
								log.Println("could not write gnb to db", err)
							} else {
								log.Println("saved gnb OK")
							}
						}
						nv.GoogleNearby = r
					} else {
						log.Println("could not googlenearby", err, "visit", nv)
					}
				}

				if qf.GoogleNearbyPhotos {
					var gnp = make(map[string]string)

					pp := tx.Bucket([]byte(googlefindnearbyphotos))

					for _, r := range nv.GoogleNearby.Results {
						if len(r.Photos) == 0 {
							continue
						}
						ref := r.Photos[0].PhotoReference
						if b64 := pp.Get([]byte(ref)); b64 != nil {
							gnp[ref] = string(b64)
						}
					}

					// if we got NOTHING, then lookup. this, this is mostly ugly ROLLOUT feature
					if len(gnp) == 0 {
						// go grab em
						// google ref photos
						placePhotos, err := nv.GoogleNearbyImagesQ()
						if err != nil {
							log.Println("could not query visit photos", err)
						} else {
							for ref, b64 := range placePhotos {
								if err := pp.Put([]byte(ref), []byte(b64)); err != nil {
									log.Println("err storing photoref:b64", err)
								}
							}
							nv.GoogleNearbyPhotos = placePhotos
						}
					} else {
						nv.GoogleNearbyPhotos = gnp
					}
				}
			}

			visits = append(visits, nv)

			// TODO MIGRATE ONLY; REMOVE ME
			// bpc := tx.Bucket([]byte(placesByCoord))
			// if stats := bpc.Stats(); stats.KeyN == 0 {
			// 	if err := storeVisitLatLng(tx, nv, vis); err != nil {
			// 		log.Println("err storing visit by lat+lng", err)
			// 		return err
			// 	}
			// }
		}
		return nil
	})

	// filter: handle reverse Chrono
	if qf.ReverseChrono {
		// sort with custom Less function in closure
		sort.Slice(visits, func(i, j int) bool {
			return visits[i].ArrivalTime.Before(visits[j].ArrivalTime)
		})
	} else {
		sort.Slice(visits, func(i, j int) bool {
			return visits[i].ArrivalTime.After(visits[j].ArrivalTime)
		})
	}

	// filter: paginate with indexes, limited oob's
	// FIXME this might not even be right, just tryna avoid OoB app killers (we could theor allow negs, with fance reversing wrapping, but tldd)
	if qf.EndIndex == 0 || qf.EndIndex > len(visits) || qf.EndIndex < 0 {
		qf.EndIndex = len(visits)
	}
	if qf.StartIndex > len(visits) {
		qf.StartIndex = len(visits)
	}
	if qf.StartIndex < 0 {
		qf.StartIndex = 0
	}

	res.Visits = visits[qf.StartIndex:qf.EndIndex]
	res.Matches = matchingN
	res.Scanned = scannedN

	out, err = json.Marshal(res)

	return
}

func TrackToPlace(tp *trackPoint.TrackPoint, visit note.NoteVisit) *geojson.Feature {
	p := geojson.NewFeature(orb.Point{visit.PlaceParsed.Lng, visit.PlaceParsed.Lat})

	props := make(map[string]interface{})
	props["Name"] = tp.Name
	props["ReportedTime"] = tp.Time
	props["ArrivalTime"] = visit.ArrivalTime
	props["DepartureTime"] = visit.DepartureTime
	props["PlaceIdentity"] = visit.PlaceParsed.Identity
	props["PlaceAddress"] = visit.PlaceParsed.Address
	props["Accuracy"] = visit.PlaceParsed.Acc

	p.Properties = props
	return p
}
