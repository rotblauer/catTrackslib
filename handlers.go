package catTrackslib

import (
	"bufio"
	"bytes"
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
			req.Body = io.NopCloser(bytes.NewBuffer(v.Value().payload))
			newReq, err := http.NewRequest(req.Method, target.String(), req.Body)
			if err != nil {
				log.Println("-> forward populate error:", err, "target:", target)
				continue targetLoop
			}
			newReq.Header = req.Header.Clone()
			newReq.Header.Set("Content-Length", strconv.Itoa(len(v.Value().payload)))

			resp, err := client.Do(newReq)
			if err != nil || resp == nil {
				log.Printf("-> forward populate: target=%s err=%q pending=%d\n", target.String(), err, cache.Len())
				continue targetLoop
			}
			log.Printf("-> forward populate: target=%s status=%s pending=%d\n", target.String(), resp.Status, cache.Len())
			if err := resp.Body.Close(); err != nil {
				log.Println("forward populate failed to close body", err, "target:", target)
				continue targetLoop
			}
			if resp.StatusCode != http.StatusOK {
				log.Println("forward populate failed, status:", resp.Status, "target:", target)
				// log the request for debugging
				if b, _ := httputil.DumpRequest(newReq, false); b != nil {
					log.Println(string(b))
				}
				continue targetLoop
			}
			cache.Delete(k)
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

	// return out
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
	trackPoints := trackPoint.TrackPoints{}
	if err := json.Unmarshal(data, &trackPoints); err == nil {
		gja2 := []*geojson.Feature{} // Its important to reset this to avoid any mutation by previous attempt.
		for _, tp := range trackPoints {
			gja2 = append(gja2, TrackToFeature(tp))
		}
		return gja2, nil
	}

	// ! FIXME This passes the test, but it doesn't work in the real world.
	arrayBytes, err := ndToJSONArray(io.NopCloser(bytes.NewBuffer(data)))
	if err != nil {
		return nil, err
	}
	log.Println("attempting decode as ndjson instead..., length:", len(arrayBytes), string(arrayBytes))

	// try to decode as ndgeojson
	gja3 := new([]*geojson.Feature) // Its important to reset this to avoid any mutation by previous attempt.
	// if err := ndjson.Unmarshal(data, gja3); err == nil {
	if err := json.Unmarshal(arrayBytes, gja3); err == nil {
		return *gja3, nil
	} else {
		log.Println("error decoding as geojson ndjson:", err)
	}

	trackPoints = trackPoint.TrackPoints{}
	// if err := ndjson.Unmarshal(data, &trackPoints); err == nil {
	if err := json.Unmarshal(arrayBytes, &trackPoints); err == nil {
		gja4 := []*geojson.Feature{} // Its important to reset this to avoid any mutation by previous attempt.
		for _, tp := range trackPoints {
			gja4 = append(gja4, TrackToFeature(tp))
		}
		return gja4, nil
	}

	return nil, errDecodeTracks
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("Decoding", len(body), "bytes")

	features, err := decodeAnythingToGeoJSON(body)
	if err != nil {
		log.Println("error decoding body", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("Decoded", len(features), "features")
	if len(features) == 0 {
		http.Error(w, "No features to populate", http.StatusBadRequest)
		return
	}

	// goroutine keeps http req from blocking while points are processed
	go func() {
		stored, errS := storePoints(features)
		if errS != nil {
			log.Println("store err:", errS)
			// http.Error(w, errS.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("Stored %d features\n", stored)
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
