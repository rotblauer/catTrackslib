package catTrackslib

import (
	"database/sql"
	"encoding/json"
	"log"

	"github.com/cridenour/go-postgis"
	"github.com/davecgh/go-spew/spew"
	_ "github.com/lib/pq"
	"github.com/rotblauer/trackpoints/trackPoint"
	bolt "go.etcd.io/bbolt"
)

var uniqueCatNames = []string{}

func collectUniqCatName(s string) {
	for _, v := range uniqueCatNames {
		if v == s {
			return
		}
	}
	uniqueCatNames = append(uniqueCatNames, s)
}

func pgTrackID(*trackPoint.TrackPoint) string {
	return ""
}

func ExportPostGIS(exportTarget string) {
	log.Println("running export...")

	log.Println("export target:", exportTarget)
	db, err := sql.Open("postgres", exportTarget)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}

	}()
	/*
		CREATE TABLE cat_tracks (
		   row_id serial PRIMARY KEY,
		   geom_point VARCHAR
		);
	*/

	_, err = db.Exec("CREATE EXTENSION IF NOT EXISTS postgis")
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	GetDB("master").View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(trackKey))
		c := b.Cursor()
		// bucketStats := b.Stats()
		// log.Printf("bucket stats: %+v", bucketStats)
		for k, v := c.First(); k != nil && len(uniqueCatNames) < 2; k, v = c.Next() {
			count++
			var track trackPoint.TrackPoint
			err := json.Unmarshal(v, &track)
			if err != nil {
				log.Println(err)
				continue
			}
			collectUniqCatName(track.Name)
			spew.Println(track)

			point := postgis.PointS{
				SRID: 4326,
				X:    track.Lng,
				Y:    track.Lat,
			}

			res, err := db.Exec("INSERT INTO cat_tracks (track_id, point) VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326))", track.ID, track.Lng, track.Lat)
			if err != nil {
				log.Fatal(err)
			}
			rowsAffected, _ := res.RowsAffected()
			log.Println("rows affected:", rowsAffected, point.GetType())
		}
		return nil
	})
	log.Printf("unique cat names: %+v", uniqueCatNames)
	log.Println("count:", count)

}
