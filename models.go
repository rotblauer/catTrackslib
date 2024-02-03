package catTrackslib

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	// Package image/jpeg is not used explicitly in the code below,
	// but is imported for its initialization side-effect, which allows
	// image.Decode to understand JPEG formatted images. Uncomment these
	// two lines to also understand GIF and PNG images:
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/groupcache/lru"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	note "github.com/rotblauer/catnotelib"
	"github.com/rotblauer/trackpoints/trackPoint"
	bolt "go.etcd.io/bbolt"
	gm "googlemaps.github.io/maps"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	catnames "github.com/rotblauer/cattracks-names"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

type LastKnown map[string]*trackPoint.TrackPoint
type LastKnownGeoJSON map[string]*geojson.Feature
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

type VisitsResponse struct {
	Visits    []*note.NoteVisit `json:"visits"`
	Stats     bolt.BucketStats  `json"bucketStats,omitempty"`
	StatsTook time.Duration     `json:"statsTook,omitempty"` // how long took to get bucket stats (for 10mm++ points, long time)
	Scanned   int               `json:"scanned"`             // num visits checked before mtaching filters
	Matches   int               `json:"matches"`             // num visits matching before paging/index filters
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

func getmetadata() (out []byte, err error) {
	err = GetDB("master").View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(statsKey))
		out = b.Get([]byte("metadata"))
		return nil
	})
	return
}
func storemetadata(lastpoint *trackPoint.TrackPoint, lenpointsupdated int) error {
	db := GetDB("master")
	e := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(statsKey))
		if err != nil {
			return err
		}

		// if not initialized, run the stats which takes a hot second
		var keyN int

		var tileDBLastUpdated time.Time
		// var homedir string
		// usr, err := user.Current()
		// if err != nil {
		// 	log.Println("get current user err", err)
		// 	homedir = os.Getenv("HOME")
		// } else {
		// 	homedir = usr.HomeDir
		// }
		// dbpath := filepath.Join(homedir, punktlichTileDBPathRelHome)
		// dbpath = filepath.Clean(dbpath)
		dbpath := GetDB("master").Path()
		log.Println("dbpath", dbpath)
		dbfi, err := os.Stat(dbpath)
		if err == nil {
			tileDBLastUpdated = dbfi.ModTime()
		} else {
			log.Println("err tile db path stat:", err)
		}

		v := b.Get([]byte("metadata"))
		md := &Metadata{}
		var keyNUpdated time.Time

		if v == nil {
			log.Println("updating bucket stats key_n...")
			keyN = tx.Bucket([]byte(trackKey)).Stats().KeyN
			log.Println("initialized metadata", "keyN:", keyN)
			keyNUpdated = time.Now().UTC()
		} else {
			if e := json.Unmarshal(v, md); e != nil {
				return e
			}
		}
		if md != nil && (md.KeyNUpdated.IsZero() || time.Since(md.KeyNUpdated) > 24*time.Hour) {
			log.Println("updating bucket stats key_n...")
			log.Println("  because", md == nil, md.KeyNUpdated, md.KeyNUpdated.IsZero(), time.Since(md.KeyNUpdated) > 24*time.Hour)
			keyN = 0
			// keyN = tx.Bucket([]byte(trackKey)).Stats().KeyN
			log.Println("updated metadata keyN:", keyN)
			keyNUpdated = time.Now().UTC()
		} else {
			log.Println("dont update keyn", md == nil, md.KeyNUpdated, md.KeyNUpdated.IsZero(), time.Since(md.KeyNUpdated) > 24*time.Hour)
			keyN = md.KeyN + lenpointsupdated
		}

		d := &Metadata{
			KeyN:               keyN,
			LastUpdatedAt:      time.Now().UTC(),
			LastUpdatedBy:      lastpoint.Name,
			LastUpdatedPointsN: lenpointsupdated,
			TileDBLastUpdated:  tileDBLastUpdated,
		}
		if !keyNUpdated.IsZero() {
			d.KeyNUpdated = keyNUpdated
		} else {
			d.KeyNUpdated = md.KeyNUpdated
		}
		by, e := json.Marshal(d)
		if e != nil {
			return nil
		}
		if e := b.Put([]byte("metadata"), by); e != nil {
			return e
		}

		return nil
	})
	return e
}

func getLastKnownData() (out []byte, err error) {
	err = GetDB("master").View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(statsKey))
		out = b.Get([]byte("lastknown"))

		// // For backwards-compatibility with the API, which expects a map of catname->TRACKPOINTs,
		// // we'll unmarshal this as a map:string::geojson.Features and then convert each entry to a trackpoint,
		// // then remarshal that to json and return it (assign to out).
		// lk := LastKnownGeoJSON{}
		// if err := json.Unmarshal(out, &lk); err != nil {
		// 	return err
		// }
		// lkOriginal := LastKnown{}
		// for name, feature := range lk {
		// 	track, err := FeatureToTrack(*feature)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	lkOriginal[name] = &track
		// }
		// out, err = json.MarshalIndent(out, "", "  ")
		// if err != nil {
		// 	return err
		// }

		return nil
	})
	return
}

func storeLastKnown(feature *geojson.Feature) {
	// lastKnownMap[feature.Name] = feature
	lk := LastKnownGeoJSON{}
	if err := GetDB("master").Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(statsKey))
		if err != nil {
			return err
		}

		v := b.Get([]byte("lastknown"))

		// Ignore the error b/c we used to store trackpoints, now using geosjon features.
		// If this fails, we'll just create a new LastKnownGeoJSON map.
		_ = json.Unmarshal(v, &lk)

		// Assign the last-known feature to this cat.
		lk[feature.Properties["Name"].(string)] = feature

		// Marshal and write the whole map.
		if by, e := json.Marshal(lk); e == nil {
			if e := b.Put([]byte("lastknown"), by); e != nil {
				return e
			}
		} else {
			log.Println("err marshalling lastknown", feature)
		}
		return nil
	}); err != nil {
		log.Printf("error storing last known: %v\n", err)
	} else {
		log.Println("Last Known 🐈 /", CatTrackGeoJSON{*feature}.String())
	}
}

type F struct {
	p  string // path to file
	f  *os.File
	gf *gzip.Writer
	je *json.Encoder
}

func CreateGZ(s string, compressLevel int) (f F) {
	fi, err := os.OpenFile(s, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Printf("Error in Create file\n")
		panic(err)
	}
	gf, err := gzip.NewWriterLevel(fi, compressLevel)
	if err != nil {
		log.Printf("Error in Create gz \n")
		panic(err)
	}
	je := json.NewEncoder(gf)
	f = F{s, fi, gf, je}
	return
}

func (f F) JE() *json.Encoder {
	return f.je
}

func CloseGZ(f F) {
	// Close the gzip first.
	f.gf.Flush()
	f.gf.Close()
	f.f.Close()
}

// func TrackToFeaturePlace(tp trackPoint.TrackPoint) *geojson.Feature {
// 	p := geojson.NewPoint(geojson.Coordinate{geojson.Coord(tp.Lng), geojson.Coord{tp.Lat}})

// 	ns, err := note.NotesField(tp.Notes).AsNoteStructured()
// 	if err != nil {
// 		return nil
// 	}

// 	if !ns.HasValidVisit() {
// 		return nil
// 	}

// 	visit, err := ns.Visit.AsVisit()
// 	if err != nil {
// 		return nil
// 	}

// 	place, err := visit.Place.AsPlace()
// 	if err != nil {
// 		return nil
// 	}

// 	props := make(map[string]interface{})
// 	props["CatName"] = tp.Name
// 	props["ArrivalTime"] = visit.ArrivalTime
// 	props["DepartureTime"] = visit.DepartureTime
// 	props["Identity"] = place.Identity
// 	props["Address"] = place.Address
// 	props["Activity"] = ns.Activity
// 	return geojson.NewFeature(p, props, 1)
// }

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
func TrackToFeature2(tp *trackPoint.TrackPoint) *geojson.Feature {
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

	if ns, e := note.NotesField(tp.Notes).AsNoteStructured(); e == nil {
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

func TrackToFeature(trackPointCurrent *trackPoint.TrackPoint) *geojson.Feature {
	// convert to a feature
	// p := geojson.NewPoint(geojson.Coordinate{geojson.Coord(trackPointCurrent.Lng), geojson.Coord(trackPointCurrent.Lat)})
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

	// not implemented yet
	if hr := trackPointCurrent.HeartRate; hr != 0 {
		props["HeartRate"] = hr
	}

	if ns, e := note.NotesField(trackPointCurrent.Notes).AsNoteStructured(); e == nil {
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
			bs := note.BatteryStatus{}
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

	} else if _, e := note.NotesField(trackPointCurrent.Notes).AsFingerprint(); e == nil {
		// maybe do something with identity consolidation?
	} else {
		// NOOP normal
		// props["Notes"] = note.NotesField(trackPointCurrent.Notes).AsNoteString()
	}

	// var currentNote note.Note
	// var currentNote note.NotesField
	// e := json.Unmarshal([]byte(trackPointCurrent.Notes), &currentNote)
	// if e != nil {
	// 	props["Notes"] = currentNote.CustomNote
	// 	props["Pressure"] = currentNote.Pressure
	// 	props["Activity"] = currentNote.Activity
	// } else {
	// 	props["Notes"] = trackPointCurrent.Notes
	// }
	return p
}

func FeatureToTrack(f geojson.Feature) (trackPoint.TrackPoint, error) {
	var err error
	tp := trackPoint.TrackPoint{}

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
	notes := note.NoteStructured{}
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

var NotifyNewEdge = make(chan bool, 1000)
var NotifyNewPlace = make(chan bool, 1000)
var FeaturePlaceChan = make(chan *geojson.Feature, 100000)

var masterGZLock sync.Mutex

func storePoints(features []*geojson.Feature) (int, error) {
	var stored = len(features)
	var err error

	if len(features) == 0 {
		return 0, errors.New("0 trackpoints to store")
	}
	// var f F
	// var fdev F
	var fedge F
	featureChan := make(chan *geojson.Feature, 100000)
	featureChanDevop := make(chan *geojson.Feature, 100000)
	featureChanEdge := make(chan *geojson.Feature, 100000)
	defer close(featureChan)
	defer close(featureChanDevop)
	defer close(featureChanEdge)
	if tracksGZPathEdge != "" {
		fedgeName := fmt.Sprintf(tracksGZPathEdge+"-wip-%d", time.Now().UnixNano())
		fedge = CreateGZ(fedgeName, gzip.BestCompression)
		go func(f F) {
			for feat := range featureChanEdge {
				if feat == nil {
					continue
				}
				f.je.Encode(feat)
			}
			CloseGZ(f)
			os.Rename(f.p, fmt.Sprintf(tracksGZPathEdge+"-fin-%d", time.Now().UnixNano()))
			NotifyNewEdge <- true
		}(fedge)
	}
	// only freya (no --proc flags, just append to master.json.gz for funsies)
	if tracksGZPath != "" && tracksGZPathEdge == "" {
		go func() {
			masterGZLock.Lock()
			defer masterGZLock.Unlock()
			mgz := CreateGZ(tracksGZPath, gzip.BestCompression)
			for feat := range featureChan {
				if feat == nil {
					continue
				}
				mgz.je.Encode(feat)
			}
			CloseGZ(mgz)
		}()
	}
	// // tracksGzpath only cuz too lazy to add another flag for places, and we'll use the tracsgz path dir
	// if tracksGZPathEdge != "" && placesLayer {
	// 	go func() {
	// 		PlacesGZLock.Lock()
	// 		defer PlacesGZLock.Unlock()
	// 		pgz := CreateGZ(filepath.Join(filepath.Dir(tracksGZPathEdge), "places.json.gz"), gzip.BestCompression)
	// 		for feat := range featurePlaceChan {
	// 			if feat == nil {
	// 				continue
	// 			}
	// 			pgz.je.Encode(feat)
	// 		}
	// 		CloseGZ(pgz)
	// 		NotifyNewPlace <- true
	// 	}()
	// }
	for _, feature := range features {
		// storePoint can modify the point, like tp.ID, tp.imgS3 field
		e := storePoint(feature)
		if e != nil {
			stored--
			log.Println("store point error: ", e)
			continue
		}
		// var t2f *geojson.Feature
		// if tracksGZPath != "" || tracksGZPathEdge != "" || tracksGZPathDevop != "" {
		// 	 t2f = TrackToFeature(point)
		// }
		if tracksGZPath != "" {
			featureChan <- feature
		}
		if tracksGZPathEdge != "" {
			featureChanEdge <- feature
		}
		// // tp has note has visit
		// if !visit.ReportedTime.IsZero() && placesLayer {
		// 	FeaturePlaceChan <- TrackToPlace(point, visit)
		// 	if err := storePointVisit(point, visit); err != nil {
		// 		log.Println("err storing point visit:", err)
		// 		continue
		// 	}
		// }
	}

	if err == nil {
		l := len(features)
		// err = storemetadata(features[l-1], l)
		storeLastKnown(features[l-1])
	}
	return stored, err
}

// func storePointVisit(point *trackPoint.TrackPoint, visit note.NoteVisit) error {
// 	// google it
// 	g, err := visit.GoogleNearbyQ()
// 	if err != nil {
// 		log.Println("google nearby failed", err)
// 		return err
// 	}
//
// 	log.Println("googleNearby OK")
//
// 	b, err := json.Marshal(g)
// 	if err != nil {
// 		log.Println("err marshalling googlenearby res", err)
// 		return err
// 	}
//
// 	if err := GetDB("master").Update(func(tx *bolt.Tx) error {
// 		pg := tx.Bucket([]byte(googlefindnearby))
// 		err = pg.Put(buildTrackpointKey(point), b)
// 		if err != nil {
// 			log.Println("could not write google response to bucket", err)
// 			return err
// 		}
// 		return nil
// 	}); err != nil {
// 		log.Println("err saving googlenearby info", err)
// 		return err
// 	}
//
// 	visit.GoogleNearby = g
// 	placePhotos, err := visit.GoogleNearbyImagesQ()
// 	if err != nil {
// 		log.Println("err could not query visit photos", err)
// 		return err
// 	}
// 	if err := GetDB("master").Update(func(tx *bolt.Tx) error {
// 		pp := tx.Bucket([]byte(googlefindnearbyphotos))
// 		var e error // only return last err (nonhalting)
// 		for ref, b64 := range placePhotos {
// 			if err := pp.Put([]byte(ref), []byte(b64)); err != nil {
// 				log.Println("err storing photoref:b64", err)
// 				e = err
// 			}
// 		}
// 		return e
// 	}); err != nil {
// 		log.Println("err saving googlenearby PHOTOS info", err)
// 		return err
// 	} else {
// 		log.Println("saved googlenearby ", len(placePhotos), "photos")
// 	}
// 	return nil
// }

func mustGetTime(f *geojson.Feature) time.Time {
	if t, ok := f.Properties["Time"].(time.Time); ok {
		return t
	}

	noTimeDefault := time.Unix(0, 0)

	if timeStr, ok := f.Properties["Time"].(string); ok {
		t, err := time.Parse(time.RFC3339, timeStr)
		if err == nil {
			return t
		}
		return noTimeDefault
	}
	return noTimeDefault
}

func buildTrackpointKey(tp *geojson.Feature) []byte {
	tpUUID, _ := tp.Properties["UUID"].(string)
	tpName, _ := tp.Properties["Name"].(string)

	// have uuid
	str := fmt.Sprintf("%s+%s+%d", tpName, tpUUID, mustGetTime(tp).Unix())
	return []byte(str)
	// k := []byte{}
	// k = append(k, []byte(tpUUID)...)
	// k = append(k, '+')
	// k = append(k, i64tob(mustGetTime(tp).Unix())...)
	// return k
}

var errDuplicatePoint = fmt.Errorf("duplicate point")

var dedupeCache = lru.New(500_000)

func validatePoint(tp *geojson.Feature) error {
	if tp == nil {
		return fmt.Errorf("nil point")
	}
	if tp.Geometry == nil {
		return fmt.Errorf("nil geometry")
	}
	if _, ok := tp.Geometry.(orb.Point); !ok {
		return fmt.Errorf("not a point")

	}
	if tp.Properties == nil {
		return fmt.Errorf("nil properties")
	}
	if tp.Properties["Name"] == nil {
		return fmt.Errorf("nil name")
	}
	if _, ok := tp.Properties["Name"].(string); !ok {
		return fmt.Errorf("name not a string")
	}
	if tp.Properties["UUID"] == nil {
		return fmt.Errorf("nil uuid")
	}
	if _, ok := tp.Properties["UUID"].(string); !ok {
		return fmt.Errorf("uuid not a string")
	}
	if tp.Properties["Time"] == nil {
		return fmt.Errorf("nil time")
	}
	if _, ok := tp.Properties["Time"]; !ok {
		return fmt.Errorf("missing field: Time")
	}
	return nil
}

func storePoint(feat *geojson.Feature) error {
	var err error

	if err := validatePoint(feat); err != nil {
		return err
	}

	tpTime := mustGetTime(feat)

	pt, ok := feat.Geometry.(orb.Point)
	if !ok {
		return fmt.Errorf("not a point: %v", feat.Geometry)
	}
	ptLng, ptLat := pt[0], pt[1]

	if ptLat > 90 || ptLat < -90 {
		return fmt.Errorf("invalid coordinate: lat=%.14f", ptLat)
	}
	if ptLng > 180 || ptLng < -180 {
		return fmt.Errorf("invalid coordinate: lng=%.14f", ptLng)
	}

	// Note that tp.ID is not the db key. ID is a uniq identifier per cat only.
	tpBoltKey := buildTrackpointKey(feat)
	feat.ID = 0

	// gets "" case nontestesing
	feat.Properties["Name"] = getTestesPrefix() + feat.Properties["Name"].(string)

	if _, ok := dedupeCache.Get(string(tpBoltKey)); ok {
		// duplicate point
		return fmt.Errorf("%w: %s", errDuplicatePoint, tpBoltKey)
	}
	dedupeCache.Add(string(tpBoltKey), true)

	// handle storing images
	if feat.Properties["imgB64"] == nil {
		return nil
	}

	// define 'key' for s3 upload
	k := fmt.Sprintf("%s_%s_%d", catnames.AliasOrSanitizedName(feat.Properties["Name"].(string)),
		feat.Properties["UUID"].(string), tpTime.Unix()) // RandStringRunes(32)
	if os.Getenv("AWS_BUCKETNAME") != "" {
		feat.Properties["imgS3"] = os.Getenv("AWS_BUCKETNAME") + "/" + k
	} else {
		// won't be an s3 url, but will have a sufficient filename for indexing
		feat.Properties["imgS3"] = k
	}

	// decode base64 -> image
	b64 := feat.Properties["imgB64"].(string)

	jpegBytes, jpegErr := b64ToJPGBytes(b64)
	if jpegErr != nil {
		log.Println("Error converting b64 to jpeg bytes: err=", jpegErr)
		return jpegErr
	}

	// remove the b64 from the properties
	delete(feat.Properties, "imgB64")

	_db := GetDB("master")

	go func() {
		// save jpg to fs
		dbRootDir := filepath.Dir(_db.Path())
		catsnapsDir := filepath.Join(dbRootDir, "catsnaps")
		os.MkdirAll(catsnapsDir, 0755)
		imagePath := filepath.Join(catsnapsDir, k+".jpg")
		if e := os.WriteFile(imagePath, jpegBytes, 0644); e != nil {
			log.Println("Error writing catsnap to fs: err=", e)
			err = e
		}
	}()
	if os.Getenv("AWS_BUCKETNAME") != "" {
		go func() {
			if e := storeImageS3(k, jpegBytes); e != nil {
				err = e
			}
		}()
	}

	err = _db.Update(func(tx *bolt.Tx) error {
		snapBuck, err := tx.CreateBucketIfNotExists([]byte(catsnapsGeoJSONKey))
		if err != nil {
			return err
		}
		featureJSON, e := json.Marshal(feat)
		if e != nil {
			log.Println("Error marshaling catsnap JSON: err=", e)
			err = e
			return err
		}
		e = snapBuck.Put(tpBoltKey, featureJSON)
		if e != nil {
			log.Println("Error storing catsnap: err=", e)
			err = e
			return err
		}
		log.Println("Saved catsnap: ", feat)
		return err
	})

	if err != nil {
		log.Println(err)
	}
	return err
}

func b64ToJPGBytes(b64 string) ([]byte, error) {
	// Decode
	unbased, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(unbased)
	im, err := jpeg.Decode(r)
	if err != nil {
		return nil, err
	}

	b := []byte{}
	buf := bytes.NewBuffer(b)
	err = jpeg.Encode(buf, im, &jpeg.Options{Quality: 100})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func storeImageS3(key string, jpegBytes []byte) (err error) {

	// S3

	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	sess := session.Must(session.NewSession())

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	timeout := time.Second * 10
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	defer cancelFn()

	bucket := os.Getenv("AWS_BUCKETNAME")
	// Uploads the object to S3. The Context will interrupt the request if the
	// timeout expires.
	_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(jpegBytes),
		ContentType:   aws.String("image/jpeg"),
		ContentLength: aws.Int64(int64(len(jpegBytes))),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
		return err
	}

	fmt.Printf("successfully uploaded file to %s/%s\n", bucket, key)
	return nil
}

func Float64frombytesBig(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Float64bytesBig(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

func storeVisitLatLng(tx *bolt.Tx, visit *note.NoteVisit, visitJSON []byte) error {
	k := Float64bytesBig(visit.PlaceParsed.Lat + 90)
	k = append(k, Float64bytesBig(visit.PlaceParsed.Lng+180)...)
	k = append(k, []byte(visit.ReportedTime.Format(time.RFC3339))...)

	pll := tx.Bucket([]byte(placesByCoord))
	err := pll.Put(k, visitJSON)
	if err != nil {
		return err
	}
	fmt.Println("Saved visit by lat/lng: ", visit)
	return nil
}

func storeVisit(tx *bolt.Tx, key []byte, visit note.NoteVisit) error {
	visitJSON, err := json.Marshal(visit)
	if err != nil {
		return fmt.Errorf("marshal visit err: %v", err)
	}

	pb, err := tx.CreateBucketIfNotExists([]byte(placesKey))
	if err != nil {
		return err
	}
	err = pb.Put(key, visitJSON)
	if err != nil {
		return err
	}
	fmt.Println("Saved visit: ", visit)

	if err := storeVisitLatLng(tx, &visit, visitJSON); err != nil {
		return err
	}
	return nil
}

func getAllStoredPoints() (tps trackPoint.TPs, e error) {
	start := time.Now()

	e = GetDB("master").View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(trackKey))

		// can swap out for- eacher if we figure indexing, or even want it
		b.ForEach(func(trackPointKey, trackPointVal []byte) error {

			var trackPointCurrent trackPoint.TrackPoint
			err := json.Unmarshal(trackPointVal, &trackPointCurrent)
			if err != nil {
				return err
			}

			tps = append(tps, &trackPointCurrent)
			return nil
		})
		return nil
	})
	fmt.Printf("Found %d points with iterator method - %s\n", len(tps), time.Since(start))

	return tps, e
}

// TODO make queryable ala which cat when
// , channel chan *trackPoint.TrackPoint
func getPointsQT(query *query) (tps trackPoint.TPs, err error) {

	if query == nil {
		query = NewQuery()
	}

	query.SetDefaults() // eps, lim  catches empty vals

	if query.IsBounded() {
		tps = getPointsFromQT(query)
	} else {
		tps, err = getAllStoredPoints()
		if err != nil {
			return nil, err
		}
	}

	if len(tps) > query.Limit {
		limitedTPs, err := limitTrackPoints(query, tps)
		if err != nil {
			fmt.Println(err)
			return tps, err
		}
		tps = limitedTPs
	}

	sort.Sort(tps)

	return tps, err
}

// TODO
// What if tracks were stored in DB gzipped? 356GB -> ? GB
// What if we duplicated or organized (indexed) gzip tracks by DAY?
// The ability to actually read tracks by time would be useful, at least for
// stuff like segment-arbitration backtesting and gps filter/smoothing backtesting.
// Is this 'export'? 'read'? 'query'? 'get'?

func migrateCatSnaps() error {
	err := GetDB("master").Update(func(tx *bolt.Tx) error {
		trackSnaps := tx.Bucket([]byte(catsnapsKey))
		geoSnaps, err := tx.CreateBucketIfNotExists([]byte(catsnapsGeoJSONKey))
		if err != nil {
			return err
		}

		trackSnaps.ForEach(func(k, v []byte) error {
			var tp *trackPoint.TrackPoint
			err := json.Unmarshal(v, &tp)
			if err != nil {
				return err
			}

			f := TrackToFeature(tp)
			fb, err := json.Marshal(f)
			if err != nil {
				return err
			}

			err = geoSnaps.Put(k, fb)
			if err != nil {
				return err
			}
			return nil
		})
		return nil
	})
	return err
}

func getCatSnaps(startTime, endTime time.Time) ([]*geojson.Feature, error) {
	// func getCatSnaps(startTime, endTime time.Time) ([]*geojson.Feature, error) {
	features := []*geojson.Feature{}
	var err error

	initGeoCatSnaps := false

	err = GetDB("master").View(func(tx *bolt.Tx) error {

		trackSnapsBucket := tx.Bucket([]byte(catsnapsKey))

		// 20240127: Migrating to geojson.Features.
		geoSnapsBucket := tx.Bucket([]byte(catsnapsGeoJSONKey))

		// initGeoCatSnaps will be used to arbitrate migration logic.
		initGeoCatSnaps = geoSnapsBucket == nil

		if geoSnapsBucket == nil {
			// Original, pre-migration.
			if err := trackSnapsBucket.ForEach(func(k, v []byte) error {
				var tp *trackPoint.TrackPoint
				err := json.Unmarshal(v, &tp)
				if err != nil {
					return err
				}

				// Skip catsnaps before parameterized start limit.
				if !startTime.IsZero() && tp.Time.Before(startTime) {
					return nil
				}
				if !endTime.IsZero() && tp.Time.After(endTime) {
					return nil
				}

				if ns, e := note.NotesField(tp.Notes).AsNoteStructured(); e != nil || !ns.HasS3Image() {
					return nil
				}

				f := TrackToFeature(tp)
				features = append(features, f)

				return nil
			}); err != nil {
				return err
			}
		} else {
			if err := geoSnapsBucket.ForEach(func(k, v []byte) error {
				feat, err := geojson.UnmarshalFeature(v)
				if err != nil {
					return err
				}

				if _, ok := feat.Properties["imgS3"]; !ok || feat.Properties["imgS3"].(string) == "" {
					return nil
				}

				t := mustGetTime(feat)

				// Skip catsnaps before parameterized start limit.
				if !startTime.IsZero() && t.Before(startTime) {
					return nil
				}
				if !endTime.IsZero() && t.After(endTime) {
					return nil
				}

				features = append(features, feat)
				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})

	if err == nil && initGeoCatSnaps {
		go func() {
			log.Println("Migrating cat snaps to geojson features")
			if err := migrateCatSnaps(); err != nil {
				log.Println("Error migrating cat snaps to geojson features", err)
			}
		}()
	}

	return features, err
}
