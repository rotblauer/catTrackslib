package catTrackslib

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"regexp"
	"strconv"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/paulmach/orb/geojson"

	"github.com/rotblauer/trackpoints/trackPoint"
	// "os"
	// "path"
)

func getData(query *query) ([]byte, error) {
	var data []byte
	allPoints, e := getPointsQT(query)
	if e != nil {
		return data, e
	}
	data, err := json.Marshal(allPoints)
	if err != nil {
		return data, err
	}
	return data, nil
}

type forwardingQueueItem struct {
	request *http.Request
	payload []byte
}

func tryForwardPopulate() {
	client := &http.Client{Timeout: time.Second * 10}

	forwardTargetRequestsLock.Lock()
	defer forwardTargetRequestsLock.Unlock()

targetLoop:
	for target, cache := range forwardTargetRequests {
		if cache.Len() == 0 {
			continue
		}

		for k, v := range cache.Items() {
			req := v.Value().request
			req.URL = &target
			req.Body = io.NopCloser(bytes.NewBuffer(v.Value().payload))
			resp, err := client.Do(req)
			if err != nil {
				log.Println("forward populate error:", err, "target:", target)
				continue targetLoop
			}
			if err := resp.Body.Close(); err != nil {
				log.Println("forward populate error (body close):", err, "target:", target)
				continue targetLoop
			}
			cache.Delete(k)
			log.Println("forward populate success:", target, "remaining:", cache.Len())
		}
	}
}

func handleForwardPopulate(r *http.Request, bod []byte) {

	if forwardTargetRequests == nil {
		log.Println("no forward targets, skipping")
		return
	}

	forwardTargetRequestsLock.Lock()
	for _, cache := range forwardTargetRequests {
		cache.Set(time.Now().UnixNano(), &forwardingQueueItem{
			request: r,
			payload: bod,
		}, ttlcache.DefaultTTL)
	}
	forwardTargetRequestsLock.Unlock()

	tryForwardPopulate()
}

// ToJSONbuffer converts some newline-delimited JSON to valid JSON buffer
func ndToJSONArray(reader io.Reader) []byte {
	// var buffer bytes.Buffer

	// buffer.Write([]byte("["))

	reg := regexp.MustCompile(`(?m)\S*`)
	out := []byte("[")
	scanner := bufio.NewScanner(reader)
	for {
		ok := scanner.Scan()
		if ok {
			sb := scanner.Bytes()
			if reg.Match(sb) {
				out = append(out, scanner.Bytes()...)
				out = append(out, []byte(",")...)
			}
			continue
		}
		break
	}
	out = bytes.TrimSuffix(out, []byte(","))
	out = append(out, []byte{byte(']'), byte('\n')}...)

	// r := bufio.NewReader(reader)
	//
	// buffer.Write([]byte("["))
	// for {
	//	bytes, err := r.ReadBytes(byte('\n'))
	//	//bytes, _, err := r.ReadLine()
	//	buffer.Write(bytes)
	//	//r.Peek(1)
	//	if err == io.EOF || string(bytes) == "" {
	//		break
	//	}
	//	buffer.Write([]byte(","))
	// }
	//
	// bu := []byte{}
	// buffer.Write(bu)
	// bu = bytes.TrimSuffix(bu, []byte(","))
	//
	// buffer.Reset()
	// buffer.Write(bu)
	//
	// //if bytes.Equal(buffer.Bytes()[buffer.Len()-1:], []byte(",")) {
	// //	buffer.UnreadByte()
	// //}
	//
	// buffer.Write([]byte("]"))
	// buffer.Write([]byte("\n"))

	return out
}

var errDecodeTracks = fmt.Errorf("could not decode as trackpoints or geojson or ndgeojson")

func decodeAnythingToGeoJSON(data []byte) ([]*geojson.Feature, error) {
	// try to decode as geojson
	gja := []*geojson.Feature{}
	if err := json.Unmarshal(data, &gja); err == nil {
		return gja, nil
	}

	// try to decode as geojson feature collection
	gjfc := geojson.NewFeatureCollection()
	if err := json.Unmarshal(data, &gjfc); err == nil {
		return gjfc.Features, nil
	}

	// try to decode as trackpoints
	gja = []*geojson.Feature{} // Its important to reset this to avoid any mutation by previous attempt.
	trackPoints := trackPoint.TrackPoints{}
	if err := json.Unmarshal(data, &trackPoints); err == nil {
		for _, tp := range trackPoints {
			gja = append(gja, TrackToFeature(tp))
		}
		return gja, nil
	}

	// try to decode as ndgeojson
	arrayData := ndToJSONArray(io.NopCloser(bytes.NewBuffer(data)))

	// geojson features
	gja = []*geojson.Feature{} // Its important to reset this to avoid any mutation by previous attempt.
	if err := json.Unmarshal(arrayData, &gja); err == nil {
		return gja, nil
	}

	// trackpoint features
	gja = []*geojson.Feature{} // Its important to reset this to avoid any mutation by previous attempt.
	trackPoints = trackPoint.TrackPoints{}
	if err := json.Unmarshal(arrayData, &trackPoints); err == nil {
		for _, tp := range trackPoints {
			gja = append(gja, TrackToFeature(tp))
		}
		return gja, nil
	}

	return gja, errDecodeTracks
}

func populatePoints(w http.ResponseWriter, r *http.Request) {
	dump, _ := httputil.DumpRequest(r, false)
	log.Println("/populate/:", string(dump))

	var body []byte
	var err error

	if r.Body == nil {
		log.Println("error: body nil")
		http.Error(w, "Please send a request body", 500)
		return
	}

	body, err = io.ReadAll(r.Body)
	if err != nil {
		log.Println("error reading body", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	features, err := decodeAnythingToGeoJSON(body)
	if err != nil {
		log.Println("error decoding body", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("populating", len(features), "features")

	// var ndbod []byte
	// err = json.Unmarshal(body, &trackPoints)
	// // err = json.NewDecoder(ioutil.NopCloser(bytes.NewBuffer(body))).Decode(&trackPoints)
	// if err != nil {
	// 	log.Println("Could not decode json as array, body length was:", len(body))
	//
	// 	// try decoding as ndjson..
	// 	ndbod = ndToJSONArray(io.NopCloser(bytes.NewBuffer(body)))
	//
	// 	log.Println("attempting decode as ndjson instead..., length:", len(ndbod), string(ndbod))
	//
	// 	// err = json.NewDecoder(&ndbuf).Decode(&trackPoints)
	// 	err = json.Unmarshal(ndbod, &trackPoints)
	// 	if err != nil {
	// 		log.Println("could not decode req as ndjson, error:", err.Error())
	//
	// 		// err = json.Unmarshal(json.RawMessage(body), &trackPoints)
	//
	// 		http.Error(w, err.Error(), http.StatusBadRequest)
	// 		return
	// 	} else {
	// 		log.Println("OK: decoded request as ndjson instead")
	// 	}
	// }
	// if len(trackPoints) != 0 && trackPoints[0].Name == "" {
	// 	log.Println("WARNING: trackpoints posted without name, trying geojson decode...")
	// 	// maybe we accidentally unmarshalled geosjon points as trackpoints
	// 	// try to unmarshal as geojson
	// 	gjfc := []geojson.Feature{}
	// 	by := body
	// 	if ndbod != nil {
	// 		by = ndbod
	// 	}
	// 	err = json.Unmarshal(by, &gjfc)
	// 	if err != nil {
	// 		log.Println("could not decode req as geojson, error:", err.Error())
	// 		http.Error(w, err.Error(), http.StatusBadRequest)
	// 		return
	// 	} else {
	// 		log.Println("OK: decoded request as geojson instead")
	// 		trackPoints = trackPoint.TrackPoints{}
	// 		for _, feat := range gjfc {
	// 			tr, err := FeatureToTrack(feat)
	// 			if err != nil {
	// 				log.Println("error converting feature to trackpoint:", err)
	// 				log.Println("body", string(by))
	// 				log.Println("feature", feat)
	// 				continue
	// 			}
	// 			trackPoints = append(trackPoints, &tr)
	// 		}
	// 	}
	// }

	// log.Println("checking token")
	// tok := os.Getenv("COTOKEN")
	// if tok == "" {
	// 	log.Println("ERROR: no COTOKEN env var set")
	// } else {
	// 	log.Println("GOODNEWS: using COTOKEN for cat verification")
	// 	// log.Println()
	// 	// if b, _ := httputil.DumpRequest(r, true); b != nil {
	// 	// 	log.Println(string(b))
	// 	// }
	// 	// log.Println()
	// 	verified := false
	// 	headerKey := "AuthorizationOfCats"
	// 	if h := r.Header.Get(headerKey); h != "" {
	// 		log.Println("using header verification...")
	// 		if h == tok {
	// 			log.Println("header OK")
	// 			verified = true
	// 		} else {
	// 			log.Println("header verification failed: ", h)
	// 		}
	// 	} else {
	// 		// catonmap.info:3001/populate?api_token=asdfasdfb
	// 		r.ParseForm()
	// 		if token := r.FormValue("api_token"); token != "" {
	// 			if token == tok {
	// 				log.Println("used token verification: OK")
	// 				verified = true
	// 			} else {
	// 				log.Println("token verification failed:", token)
	// 				verified = true
	// 			}
	// 		}
	// 	}
	// 	if verified {
	// 		log.Println("GOODNEWS: verified cattracks posted remote.host:", r.RemoteAddr)
	// 	} else {
	// 		trackPoints.Unverified(r)
	// 		log.Println("WARNING: unverified cattracks posted remote.host:", r.RemoteAddr)
	// 	}
	// }

	// goroutine keeps http req from blocking while points are processed
	go func() {
		errS := storePoints(features)
		if errS != nil {
			log.Println("store err:", errS)
			// http.Error(w, errS.Error(), http.StatusInternalServerError)
			return
		}
		log.Println("stored features", "len:", len(features))
	}()

	// return empty json of empty trackpoints to not have to download tons of shit
	if errW := json.NewEncoder(w).Encode([]struct{}{}); errW != nil {
		log.Println("respond write err:", errW)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func getLastKnown(w http.ResponseWriter, r *http.Request) {
	b, e := getLastKnownData()
	// b, e := json.Marshal(lastKnownMap)
	if e != nil {
		log.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Println("Got lastknown:", len(b), "bytes")
	w.Write(b)
}

func handleGetCatSnaps(w http.ResponseWriter, r *http.Request) {
	var startQ, endQ time.Time
	startRaw, ok := r.URL.Query()["tstart"]
	if ok && len(startRaw) > 0 {
		i64, err := strconv.ParseInt(startRaw[0], 10, 64)
		if err == nil {
			startQ = time.Unix(i64, 0)
		} else {
			log.Printf("catsnaps: Invalid t-start value: %s (%v)\n", startRaw[0], err)
		}
	}
	endRaw, ok := r.URL.Query()["tend"]
	if ok && len(endRaw) > 0 {
		i64, err := strconv.ParseInt(endRaw[0], 10, 64)
		if err == nil {
			endQ = time.Unix(i64, 0)
		} else {
			log.Printf("catsnaps: Invalid t-end value: %s (%v)\n", endRaw[0], err)
		}
	}
	snapPoints, e := getCatSnaps(startQ, endQ)
	if e != nil {
		log.Println(e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
	}

	bs, err := json.Marshal(snapPoints)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	fmt.Println("Got catsnaps", len(snapPoints), "snaps", len(bs), "bytes")
	w.Write(bs)
}
