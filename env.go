package catTrackslib

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	bolt "go.etcd.io/bbolt"
)

const (
	testesPrefix = "testes-------"
)

var testes = false
var forwardTargetRequests map[url.URL]*ttlcache.Cache[int64, *forwardingQueueItem]
var forwardTargetRequestsLock = sync.Mutex{}

var tracksGZPath string
var tracksGZPathDevop string
var tracksGZPathEdge string

var masterdbpath string
var devopdbpath string
var edgedbpath string

var placesLayer bool

var (
	masterlock, devoplock, edgelock string
)

// SetTestes run
func SetTestes(flagOption bool) {
	testes = flagOption
}

// SetForwardPopulate sets the 'downstream' urls that should be forwarded
// any request that this client receives for populating points. Forward requests
// will be sent as POST requests in identical JSON as they are received.
// NOTE that forwardPopulate is a []string, so all uri's should be given in comma-separated
// format.
func SetForwardPopulate(arguments string) {
	forwardTargetRequests = make(map[url.URL]*ttlcache.Cache[int64, *forwardingQueueItem])
	for _, rawURI := range strings.Split(arguments, ",") {
		if rawURI == "" {
			continue
		}
		uri, e := url.Parse(rawURI)
		if e != nil {
			panic(e)
		}
		forwardTargetRequests[*uri] = ttlcache.New[int64, *forwardingQueueItem](
			ttlcache.WithTTL[int64, *forwardingQueueItem](24 * time.Hour),
		)
		// Start the cache cleaner
		go forwardTargetRequests[*uri].Start()
		log.Println("-> Set forwarding target:", uri)
	}

	// Run an asynchronous loop repeating attempts to forward pending requests.
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		for {
			select {
			case <-ticker.C:
				tryForwardPopulate()
			}
		}
	}()
}

func SetLiveTracksGZ(pathto string) {
	tracksGZPath = pathto
}

func SetLiveTracksGZDevop(pathto string) {
	tracksGZPathDevop = pathto
}

func SetLiveTracksGZEdge(pathto string) {
	tracksGZPathEdge = pathto
}

func SetMasterLock(pathto string) {
	masterlock = pathto
}
func SetDevopLock(pathto string) {
	devoplock = pathto
}
func SetEdgeLock(pathto string) {
	edgelock = pathto
}

func SetDBPath(whichdb, pathto string) {
	switch whichdb {
	case "master", "":
		masterdbpath = pathto
	case "devop":
		devopdbpath = pathto
	case "edge":
		edgedbpath = pathto
	default:
		panic("invalid db name")
	}
}

func SetPlacesLayer(b bool) {
	placesLayer = b
}

func getTestesPrefix() string {
	if testes {
		return testesPrefix
	}
	return ""
}

// DeleteTestes wipes the entire database of all points with names prefixed with testes prefix. Saves an rm keystorke
func DeleteTestes() error {
	e := GetDB("master").Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(trackKey))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var tp TrackPoint
			e := json.Unmarshal(v, &tp)
			if e != nil {
				fmt.Println("Error deleting testes.")
				return e
			}
			if strings.HasPrefix(tp.Name, testesPrefix) {
				b.Delete(k)
			}
		}
		return nil
	})
	return e
}
