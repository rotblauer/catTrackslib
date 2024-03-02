package catTrackslib

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/paulmach/orb/geojson"
	catnames "github.com/rotblauer/cattracks-names"
	// "os"
	// "path"
)

type forwardingQueueItem struct {
	request *http.Request
	payload []byte
}

// forwardPopulateLastRun tracks the last time the function was called.
// This allows the periodic go routine to check if it should run.
var forwardPopulateLastRun = time.Unix(0, 0)

// tryForwardPopulate is a function that attempts to forward populate requests to the configured
// targets. It is called by handleForwardPopulate, and is also called by a go routine in handleForwardPopulate.
// It assumes that the forwardTargetRequestsLock is already locked, since it may edit
// the cache.
func tryForwardPopulate() {
	forwardPopulateLastRun = time.Now()

	client := &http.Client{Timeout: time.Second * 10}

targetLoop:
	for target, cache := range forwardTargetRequests {
		if cache.Len() == 0 {
			continue
		}

		// prox := httputil.NewSingleHostReverseProxy(&target)
		//
		// /*
		// 	Mar 02 15:07:40 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:07:40 -> Set forwarding target: https://nyc-subway-stations-298512.uc.r.appspot.com/populate
		// 	Mar 02 15:07:40 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:07:40 To serve and protest port 3001
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 Decoding 50535 bytes:  {"type":"FeatureCollection","features":[{"type":"Feature","properties":{"UUID":" ...
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 Decoded 60 features
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 DEBUG DIRECTOR TARGET https://nyc-subway-stations-298512.uc.r.appspot.com/populate
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 Last Known 🐈 / 🔴 Name: tonga-moto-63b2 (ia), Coords: [-111.6902417 45.5710283], Time: 2024-03-02T15:08:04.626Z, Accuracy: 3.9, Speed: 0
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 Stored 60 features
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 [procedge] starting iter
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 [procedge] bash: cat /mnt/cattracks_volume/tdata/*-fin-* >> /mnt/cattracks_volume/tdata/edge.json.gz
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: ::1->173.236.136.77 - - [02/Mar/2024:15:08:06 +0000] "POST /populate/ HTTP/1.1" 200 3
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 [procedge] bash: cat /mnt/cattracks_volume/tdata/*-fin-* >> /mnt/cattracks_volume/tdata/direct-master.json.gz
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 [procedge] rm /mnt/cattracks_volume/tdata/*-fin-*
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 [procedge] bash: cp /mnt/cattracks_volume/tdata/edge.json.gz /mnt/cattracks_volume/tdata/edge.snap.json.gz
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 > [ catTrackEdge ] /usr/local/bin/tippecanoe [--maximum-tile-bytes 330000 --cluster-densest-as-needed --cluster-distance=1 --calculate-feature-density --include Alias --include UUID --include Name --include Activity --include Elevation --include Speed --include Accuracy --include UnixTime -EUnixTime:max -EElevation:max -ESpeed:max -EAccuracy:mean --single-precision -r1 --minimum-zoom 3 --maximum-zoom 18 --json-progress --progress-interval 30 -l catTrackEdge -n catTrackEdge -o /mnt/cattracks_volume/tdata/edge.mbtiles --force --read-parallel /mnt/cattracks_volume/tdata/edge.snap.json.gz]
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 -> forward populate: target= https://nyc-subway-stations-298512.uc.r.appspot.com/populate status.code= 404 pending= 0
		// 	Mar 02 15:08:06 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878332]: 2024/03/02 15:08:06 DEBUG code= 404 r.Method= POST r.URL= /populate/
		//
		//
		// 	Mar 02 15:13:34 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:34 -> Set forwarding target: https://nyc-subway-stations-298512.uc.r.appspot.com/populate
		// 	Mar 02 15:13:34 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:34 To serve and protest port 3001
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 Decoding 98926 bytes:  [{"heading":64.933578491210938,"speed":31.439107894897461,"uuid":"05C63745-BFA3- ...
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 Decoded 100 features
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 DEBUG DIRECTOR TARGET https://nyc-subway-stations-298512.uc.r.appspot.com/populate
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 Last Known 🐈 / 🔵 Name: Rye13 (rye), Coords: [-93.0697021484375 45.19349670410156], Time: 2024-03-02 15:13:40.999 +0000 UTC, Accuracy: 4.72, Speed: 32.905
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 Stored 100 features
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 [procedge] starting iter
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 [procedge] bash: cat /mnt/cattracks_volume/tdata/*-fin-* >> /mnt/cattracks_volume/tdata/edge.json.gz
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: ::1->173.236.136.77 - - [02/Mar/2024:15:13:41 +0000] "POST /populate/ HTTP/1.1" 200 3
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 [procedge] bash: cat /mnt/cattracks_volume/tdata/*-fin-* >> /mnt/cattracks_volume/tdata/direct-master.json.gz
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 [procedge] rm /mnt/cattracks_volume/tdata/*-fin-*
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 [procedge] bash: cp /mnt/cattracks_volume/tdata/edge.json.gz /mnt/cattracks_volume/tdata/edge.snap.json.gz
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 > [ catTrackEdge ] /usr/local/bin/tippecanoe [--maximum-tile-bytes 330000 --cluster-densest-as-needed --cluster-distance=1 --calculate-feature-density --include Alias --include UUID --include Name --include Activity --include Elevation --include Speed --include Accuracy --include UnixTime -EUnixTime:max -EElevation:max -ESpeed:max -EAccuracy:mean --single-precision -r1 --minimum-zoom 3 --maximum-zoom 18 --json-progress --progress-interval 30 -l catTrackEdge -n catTrackEdge -o /mnt/cattracks_volume/tdata/edge.mbtiles --force --read-parallel /mnt/cattracks_volume/tdata/edge.snap.json.gz]
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 -> forward populate: target= https://nyc-subway-stations-298512.uc.r.appspot.com/populate status.code= 404 pending= 0
		// 	Mar 02 15:13:41 cattracks-debian-s-2vcpu-2gb-sfo3-01 cattracks[1878852]: 2024/03/02 15:13:41 DEBUG code= 404 r.Method= POST r.URL= https://nyc-subway-stations-298512.uc.r.appspot.com/populate
		//
		// */
		//
		// // Write a custom director so that we can modify the target request URL
		// // so that we don't proxy from OUR path (ie. /populate/) the target (which might want, say, /populate, or /api/apopulate, or whatever).
		// // So basically we want to make sure that whatever the --forward-url was configured to gets used literally.
		// prox.Director = func(req *http.Request) {
		// 	targetQuery := target.RawQuery
		// 	req.URL.Scheme = target.Scheme
		// 	req.URL.Host = target.Host
		// 	// 	req.URL.Path, req.URL.RawPath = joinURLPath(target, req.URL) <- This is default. (see reverseproxy.go)
		// 	req.URL.Path, req.URL.RawPath = target.Path, target.RawPath
		// 	if targetQuery == "" || req.URL.RawQuery == "" {
		// 		req.URL.RawQuery = targetQuery + req.URL.RawQuery
		// 	} else {
		// 		req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		// 	}
		//
		// 	log.Println("DEBUG DIRECTOR TARGET", req.URL.String())
		//
		// 	// And finally set our special header to indicate the original requestor just in case subsequent reverse proxies fuck it up.
		// 	req.Header.Set("Cat-Forwarded-For", req.RemoteAddr)
		// }
		//
		// // Configure the reverse proxy to use HTTPS
		// // if target.Scheme == "https" {
		// // 	prox.Transport = &http.Transport{
		// // 		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		// // 	}
		// // }

		for k, v := range cache.Items() {
			// r := v.Value().request.Clone(context.Background())
			// // r.Host = target.Host
			// r.URL = &target
			// r.Body = io.NopCloser(bytes.NewBuffer(v.Value().payload))
			// // r.Header.Set("Cat-Forwarded-For", r.RemoteAddr)
			//
			// rw := httptest.NewRecorder()
			// prox.ServeHTTP(rw, r)
			// if !rw.Flushed {
			// 	rw.Flush()
			// }
			//
			// if rw.Result() != nil && rw.Result().StatusCode < http.StatusInternalServerError {
			// 	cache.Delete(k)
			// 	log.Println("-> forward populate: target=", target.String(), "status.code=", rw.Result().StatusCode, "pending=", cache.Len())
			// 	if rw.Result().StatusCode >= 400 {
			// 		log.Println("DEBUG code=", rw.Result().StatusCode, "r.Method=", r.Method, "r.URL=", r.URL.String())
			// 	}
			// } else if rw.Result() != nil {
			// 	log.Println("-> forward populate: target=", target.String(), "status.code=", rw.Result().StatusCode, "pending=", cache.Len())
			// 	continue targetLoop
			// } else {
			// 	log.Println("-> forward populate: target=", target.String(), "NIL RESPONSE", "pending=", cache.Len())
			// 	continue targetLoop
			// }

			req := v.Value().request.Clone(context.Background())
			req.Body = io.NopCloser(bytes.NewBuffer(v.Value().payload))

			req.URL = &target
			req.Host = target.Host
			req.Body = io.NopCloser(bytes.NewBuffer(v.Value().payload))
			req.ContentLength = int64(len(v.Value().payload))
			req.Header.Set("Content-Length", strconv.Itoa(len(v.Value().payload)))
			req.Header.Set("X-Forwarded-For", req.RemoteAddr)
			req.Header.Set("Cat-Forwarded-For", req.RemoteAddr)

			newReq, err := http.NewRequest(req.Method, target.String(), req.Body)
			if err != nil {
				log.Println("-> forward populate error:", err, "target:", target, "k:", k)
				continue targetLoop
			}
			newReq.Header = req.Header.Clone()
			newReq.Header.Set("Content-Length", strconv.Itoa(len(v.Value().payload)))
			newReq.Header.Set("X-Forwarded-For", req.RemoteAddr)
			newReq.Header.Set("Cat-Forwarded-For", req.RemoteAddr)

			resp, err := client.Do(newReq)
			if err != nil || resp == nil {
				log.Printf("-> forward populate: k=%v target=%s err=%q pending=%d\n", k, target.String(), err, cache.Len())
				continue targetLoop
			}
			if err := resp.Body.Close(); err != nil {
				log.Println("forward populate failed to close body", err, "target:", target, "k:", k)
				continue targetLoop
			}

			if resp.StatusCode >= http.StatusBadRequest {
				log.Println("forward populate failed, status:", resp.Status, "target:", target)
				// log the request for debugging
				if b, _ := httputil.DumpRequest(newReq, false); b != nil {
					log.Println(string(b))
				}
			}
			// If the response was GREATER THAN a 400, continue (persisting the cache value).
			// Otherwise, it's either a 400 or < 400, so we'll delete the cache value.
			if resp.StatusCode > http.StatusBadRequest {
				continue targetLoop
			}
			cache.Delete(k)
			log.Printf("-> forward populate: k=%v target=%s status=%s pending=%d\n", k, target.String(), resp.Status, cache.Len())
		}
	}
}

func handleForwardPopulate(r *http.Request, bod []byte) {
	forwardTargetRequestsLock.Lock()
	defer forwardTargetRequestsLock.Unlock()

	for _, cache := range forwardTargetRequests {
		cache.Set(time.Now().UnixNano(), &forwardingQueueItem{
			request: r,
			payload: bod,
		}, ttlcache.DefaultTTL)
	}

	tryForwardPopulate()
}

// ToJSONbuffer converts some newline-delimited JSON to valid JSON buffer
func ndToJSONArray(reader io.Reader) ([]byte, error) {

	breader := bufio.NewReader(reader)
	wbuf := bytes.NewBuffer([]byte{})
	bwriter := bufio.NewWriter(wbuf)

readloop:
	for {
		read, err := breader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, os.ErrClosed) || errors.Is(err, io.EOF) {
				break readloop
			}
			return nil, err
		}
		bwriter.Write(read)
		bwriter.Write([]byte(","))
	}
	bwriter.Flush()
	wrote := wbuf.Bytes()
	wrote = bytes.TrimSuffix(wrote, []byte(","))
	// wrote = append([]byte{'['}, wrote...)
	// wrote = append(wrote, ']')
	return bytes.Join([][]byte{{'['}, wrote, {']'}}, nil), nil
	// return wrote, nil

	// var buffer bytes.Buffer

	// buffer.Write([]byte("["))

	// reg := regexp.MustCompile(`(?m)\S*`)
	// out := []byte("[")
	// scanner := bufio.NewScanner(reader)
	// for {
	// 	ok := scanner.Scan()
	// 	if ok {
	// 		sb := scanner.Bytes()
	// 		if reg.Match(sb) {
	// 			out = append(out, scanner.Bytes()...)
	// 			out = append(out, []byte(",")...)
	// 		}
	// 		continue
	// 	}
	// 	break
	// }
	// out = bytes.TrimSuffix(out, []byte(","))
	// out = append(out, []byte{byte(']'), byte('\n')}...)
	// return out
}

var ErrDecodeTracks = fmt.Errorf("could not decode as trackpoints or geojson or geojsonfc or ndtrackpoints or ndgeojson")

func DecodeTrackPoints(data []byte) (TrackPoints, error) {
	trackPoints := TrackPoints{}
	if err := json.Unmarshal(data, &trackPoints); err != nil {
		return nil, err
	}
	if len(trackPoints) > 0 {
		if trackPoints[0].Time.IsZero() {
			return nil, errors.New("invalid trackpoint (missing or zero 'time' field)")
		}
		return trackPoints, nil
	}
	return nil, errors.New("empty trackpoints")
}

func DecodeAnythingToGeoJSON(data []byte) ([]*geojson.Feature, error) {
	// try to decode as trackpoints
	if trackPoints, err := DecodeTrackPoints(data); err == nil {
		gja := []*geojson.Feature{}
		for _, tp := range trackPoints {
			gja = append(gja, TrackToFeature(tp))
		}
		return gja, nil
	}

	// try to decode as geojson feature collection
	gjfc := geojson.NewFeatureCollection()
	if err := gjfc.UnmarshalJSON(data); err == nil {
		return gjfc.Features, nil
	}

	// try to decode as geojson
	gja := []*geojson.Feature{}
	if err := json.Unmarshal(data, &gja); err == nil {
		return gja, nil
	}

	// ! FIXME This passes the test, but it doesn't work in the real world.
	arrayBytes, err := ndToJSONArray(io.NopCloser(bytes.NewBuffer(data)))
	if err != nil {
		return nil, err
	}
	// log.Println("attempting decode as ndjson instead..., length:", len(arrayBytes), string(arrayBytes))

	// try to decode as ndgeojson
	gja3 := new([]*geojson.Feature) // Its important to reset this to avoid any mutation by previous attempt.
	// if err := ndjson.Unmarshal(data, gja3); err == nil {
	if err := json.Unmarshal(arrayBytes, gja3); err == nil {
		return *gja3, nil
	}

	// if err := ndjson.Unmarshal(data, &trackPoints); err == nil {
	if trackPoints, err := DecodeTrackPoints(arrayBytes); err == nil {
		gja4 := []*geojson.Feature{} // Its important to reset this to avoid any mutation by previous attempt.
		for _, tp := range trackPoints {
			gja4 = append(gja4, TrackToFeature(tp))
		}
		return gja4, nil
	}

	return nil, ErrDecodeTracks
}

func populatePoints(w http.ResponseWriter, r *http.Request) {

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
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	if len(body) > 80 {
		log.Println("Decoding", len(body), "bytes: ", string(body)[:80], "...")
	} else {
		log.Println("Decoding", len(body), "bytes: ", string(body))
	}

	features, err := DecodeAnythingToGeoJSON(body)
	if err != nil {
		log.Println("error decoding body", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	log.Println("Decoded", len(features), "features")

	// Short circuit if no points decoded.
	if len(features) == 0 {
		http.Error(w, "No features to populate", http.StatusUnprocessableEntity)
		return
	}

	if forwardTargetRequests != nil {
		go handleForwardPopulate(r, body)
	}

	if err := validatePoint(features[0]); err == nil {
		catname := catnames.AliasOrSanitizedName(features[0].Properties["Name"].(string))
		if err := storeLastPushByCat(catname, body); err != nil {
			log.Println("store last push err:", err)
		}
	}

	if err := validatePopulateFeatures(features); err != nil {
		log.Println("validate features err:", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	// goroutine keeps http req from blocking while points are processed
	stored, errS := storePoints(features)
	if errS != nil {
		log.Println("store err:", errS)
		// http.Error(w, errS.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Stored %d features\n", len(stored))

	if len(stored) > 0 {
		catname := catnames.AliasOrSanitizedName(features[0].Properties["Name"].(string))
		lastPushTTLCache.Set(catname, stored, ttlcache.DefaultTTL)

		if m := GetMelody(); m != nil {
			broadcast := broadcats{Action: websocketActionPopulate, Features: stored}
			b, _ := json.Marshal(broadcast)
			m.Broadcast(b)
		}
	}

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
