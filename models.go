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
	// Package image/jpeg is not used explicitly in the code below,
	// but is imported for its initialization side-effect, which allows
	// image.Decode to understand JPEG formatted images. Uncomment these
	// two lines to also understand GIF and PNG images:
	_ "image/gif"
	"image/jpeg"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/groupcache/lru"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	catnames "github.com/rotblauer/cattracks-names"
	bolt "go.etcd.io/bbolt"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type LastKnown map[string]*TrackPoint
type LastKnownGeoJSON map[string]*geojson.Feature

func getLastKnownData() (out []byte, err error) {
	err = GetDB("master").View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(statsKey))
		out = b.Get([]byte("lastknown"))
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

var NotifyNewEdge = make(chan bool, 1000)
var NotifyNewPlace = make(chan bool, 1000)
var FeaturePlaceChan = make(chan *geojson.Feature, 100000)

var masterGZLock sync.Mutex

func storePoints(features []*geojson.Feature) ([]*geojson.Feature, error) {
	var err error

	if len(features) == 0 {
		return nil, errors.New("0 trackpoints to store")
	}

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

	// Validate all features, splicing out any invalid ones.
	// Invalid ones will be logged (with their error), but not stored.
	// https://stackoverflow.com/a/20551116/4401322 How to remove items from a slice while ranging it.
	i := 0 // output index
	for _, x := range features {
		if err := validatePoint(x); err == nil {
			// copy and increment index
			features[i] = x
			i++
		} else {
			dump := spew.Sdump(x)
			log.Println("validatePoint error: ", err, "feature: ", dump)
		}
	}
	// Prevent memory leak by erasing truncated values
	// (not needed if values don't contain pointers, directly or indirectly)
	for j := i; j < len(features); j++ {
		features[j] = nil
	}
	features = features[:i]

	// Sort the features by time (minimum increment 1 second), then by accuracy.
	// This is important for the deduplication process, which will always accept the first of any duplicate set.
	sort.Slice(features, func(i, j int) bool {
		ti := mustGetTime(features[i])
		tj := mustGetTime(features[j])
		if ti.Unix() == tj.Unix() {
			ai := features[i].Properties["Accuracy"].(float64)
			aj := features[j].Properties["Accuracy"].(float64)
			return ai < aj
		}
		return ti.Before(tj)
	})

	stored := []*geojson.Feature{}
	for _, feature := range features {
		// storePoint can modify the point, like tp.ID, tp.imgS3 field
		e := storePoint(feature)
		if e != nil {
			log.Println("store point error: ", e)
			continue
		}
		stored = append(stored, feature)
		if tracksGZPath != "" {
			featureChan <- feature
		}
		if tracksGZPathEdge != "" {
			featureChanEdge <- feature
		}
	}

	if err == nil {
		l := len(features)
		// err = storemetadata(features[l-1], l)
		storeLastKnown(features[l-1])
	}
	return stored, err
}

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
	// We know these exist because validatePoint assures that they exist and are strings.
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
	if pt, ok := tp.Geometry.(orb.Point); !ok {
		return fmt.Errorf("not a point")
	} else {
		ptLng, ptLat := pt[0], pt[1]

		if ptLat > 90 || ptLat < -90 {
			return fmt.Errorf("invalid coordinate: lat=%.14f", ptLat)
		}
		if ptLng > 180 || ptLng < -180 {
			return fmt.Errorf("invalid coordinate: lng=%.14f", ptLng)
		}
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
	if v, ok := tp.Properties["Accuracy"]; !ok {
		return fmt.Errorf("missing field: Accuracy")
	} else if _, ok := v.(float64); !ok {
		return fmt.Errorf("accuracy not a float64")
	}
	return nil
}

// validatePopulateFeatures checks that all features are valid AS A GROUP.
// (Validation of individual features is done in validatePoint.)
// So this aserts the 'integrity' of a 'batch' of features.
// We assert that:
// - all tracks pushed in a request to populate belong to the same cat
func validatePopulateFeatures(features []*geojson.Feature) error {
	name := features[0].Properties["Name"].(string)
	uuid := features[0].Properties["UUID"].(string)
	for _, f := range features {
		if f.Properties["Name"].(string) != name {
			return fmt.Errorf("name mismatch (all features must belong to same cat)")
		}
		if f.Properties["UUID"].(string) != uuid {
			return fmt.Errorf("UUID mismatch (all features must belong to same cat)")
		}
	}
	return nil
}

func storePoint(feat *geojson.Feature) error {
	var err error

	tpTime := mustGetTime(feat)

	// Note that tp.ID is not the db key. ID is a uniq identifier per cat only.
	tpBoltKey := buildTrackpointKey(feat)
	feat.ID = 0

	// gets "" case nontestesing
	feat.Properties["Name"] = getTestesPrefix() + feat.Properties["Name"].(string)

	hash, err := hashstructure.Hash(feat, hashstructure.FormatV2, nil)
	if err != nil {
		return err
	}

	dedupeKey := fmt.Sprintf("%d", hash)
	if _, ok := dedupeCache.Get(dedupeKey); ok {
		// duplicate point
		dump := spew.Sdump(feat)
		return fmt.Errorf("%w: %d\n%s\n", errDuplicatePoint, hash, dump)
	}
	dedupeCache.Add(dedupeKey, true)

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
			var tp *TrackPoint
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
				var tp *TrackPoint
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

				if ns, e := NotesField(tp.Notes).AsNoteStructured(); e != nil || !ns.HasS3Image() {
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

// storeLastPushByCat stores the data of the last populate request.
// It assumes that populate requests are unique to each cat, which may not always be true for tests/development.
func storeLastPushByCat(catName string, data []byte) error {
	datadir := filepath.Dir(tracksGZPath)
	lastPushesDatadir := filepath.Join(datadir, "lastpushes")
	if err := os.MkdirAll(lastPushesDatadir, 0755); err != nil {
		return err
	}
	lastPushesPath := filepath.Join(lastPushesDatadir, catName+".json")
	return os.WriteFile(lastPushesPath, data, 0644)
}
