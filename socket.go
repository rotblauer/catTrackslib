package catTrackslib

import (
	"encoding/json"
	"log"

	"github.com/olahol/melody"
	"github.com/paulmach/orb/geojson"
)

type websocketAction string

var websocketActionPopulate websocketAction = "populate"

type broadcats struct {
	Action   websocketAction    `json:"action"`
	Features []*geojson.Feature `json:"features"`
}

var m *melody.Melody

// InitMelody sets up the websocket handler.
func InitMelody() *melody.Melody {
	m = melody.New()

	// Incoming message about updated query params.
	m.HandleConnect(func(s *melody.Session) {
		log.Println("[websocket] connected", s.Request.RemoteAddr)
		for _, v := range lastPushTTLCache.Items() {
			features := v.Value()
			bc := broadcats{
				Action:   websocketActionPopulate,
				Features: features,
			}
			b, _ := json.Marshal(bc)
			s.Write(b)
		}
	})
	m.HandleDisconnect(func(s *melody.Session) {
		log.Println("[websocket] disconnected", s.Request.RemoteAddr)
	})
	m.HandleError(func(s *melody.Session, e error) {
		log.Println("[websocket] error", e, s.Request.RemoteAddr)
	})
	m.HandleMessage(messageHandler)
	return m
}

// GetMelody does stuff
func GetMelody() *melody.Melody {
	return m
}

// on request
func messageHandler(s *melody.Session, msg []byte) {
	log.Println("[websocket] message", string(msg))
}

// on message

// on connect?
