package catTrackslib

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNotesField_AsNoteStructured(t *testing.T) {
	type field struct {
		Note string
	}
	wantTime, _ := time.Parse(time.RFC3339, "2023-12-16T15:25:40.335Z")
	pointOfF := func(f float64) *float64 {
		return &f
	}
	pointOfI := func(f int) *int {
		return &f
	}
	tests := []struct {
		name   string
		fields field
		want   *NoteStructured
	}{
		{
			name: "test 1",
			fields: field{
				Note: "{\"floorsAscended\":56,\"customNote\":\"\",\"heartRateS\":\"\",\"currentTripStart\":\"2023-12-16T15:25:40.335Z\",\"floorsDescended\":40,\"averageActivePace\":0.53959662255944008,\"networkInfo\":\"{}\",\"numberOfSteps\":65066,\"visit\":\"{\\\"validVisit\\\":false}\",\"relativeAltitude\":-16.205322265625,\"currentCadence\":1.851201057434082,\"heartRateRawS\":\"\",\"batteryStatus\":\"{\\\"level\\\":0.94999998807907104,\\\"status\\\":\\\"unplugged\\\"}\",\"activity\":\"Stationary\",\"currentPace\":0.69526106119155884,\"imgb64\":\"\",\"pressure\":97.975234985351562,\"distance\":69266.793879101984,\"user_accelerometer_x\":0.14,\"activity_confidence\":100}",
			},
			want: &NoteStructured{
				Activity:           "Stationary",
				ActivityConfidence: pointOfI(100),
				FloorsAscended:     56,
				FloorsDescended:    40,
				NumberOfSteps:      65066,
				CurrentCadence:     1.851201057434082,
				CurrentPace:        0.69526106119155884,
				AverageActivePace:  0.53959662255944008,
				Pressure:           97.975234985351562,
				Distance:           69266.793879101984,
				CurrentTripStart:   wantTime,
				NetworkInfo:        "{}",
				HeartRateS:         "",
				HeartRateRawS:      "",
				BatteryStatus:      `{\"level\":0.94999998807907104,\"status\":\"unplugged\"}`,
				Visit:              `{\"validVisit\":false}`,
				CustomNote:         "",
				UserAccelerometer:  UserAccelerometer{X: pointOfF(0.14)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			bf := []byte(tt.fields.Note)
			nv := NotesField(bf)
			n := &nv

			if got, err := n.AsNoteStructured(); err == nil {
				if got.FloorsAscended != tt.want.FloorsAscended {
					t.Errorf("AsNoteStructured() = %v, want %v", got.FloorsAscended, tt.want.FloorsAscended)
				}
				if got.FloorsDescended != tt.want.FloorsDescended {
					t.Errorf("AsNoteStructured() = %v, want %v", got.FloorsDescended, tt.want.FloorsDescended)
				}
				if got.NumberOfSteps != tt.want.NumberOfSteps {
					t.Errorf("AsNoteStructured() = %v, want %v", got.NumberOfSteps, tt.want.NumberOfSteps)
				}
				if got.CurrentCadence != tt.want.CurrentCadence {
					t.Errorf("AsNoteStructured() = %v, want %v", got.CurrentCadence, tt.want.CurrentCadence)
				}
				if got.CurrentPace != tt.want.CurrentPace {
					t.Errorf("AsNoteStructured() = %v, want %v", got.CurrentPace, tt.want.CurrentPace)
				}
				if got.AverageActivePace != tt.want.AverageActivePace {
					t.Errorf("AsNoteStructured() = %v, want %v", got.AverageActivePace, tt.want.AverageActivePace)
				}
				if got.Pressure != tt.want.Pressure {
					t.Errorf("AsNoteStructured() = %v, want %v", got.Pressure, tt.want.Pressure)
				}
				if got.Distance != tt.want.Distance {
					t.Errorf("AsNoteStructured() = %v, want %v", got.Distance, tt.want.Distance)
				}
				if got.CurrentTripStart != tt.want.CurrentTripStart {
					t.Errorf("AsNoteStructured() = %v, want %v", got.CurrentTripStart, tt.want.CurrentTripStart)
				}
				if got.Activity != tt.want.Activity {
					t.Errorf("AsNoteStructured() = %v, want %v", got.Activity, tt.want.Activity)
				}
				if *got.ActivityConfidence != *tt.want.ActivityConfidence {
					t.Errorf("AsNoteStructured() = %v, want %v", got.ActivityConfidence, tt.want.ActivityConfidence)
				}
				if *got.UserAccelerometer.X != *tt.want.UserAccelerometer.X {
					t.Errorf("AsNoteStructured() = %v, want %v", got.UserAccelerometer.X, tt.want.UserAccelerometer.X)
				}

				show, err := json.MarshalIndent(got, "", "  ")
				if err != nil {
					t.Errorf("AsNoteStructured() = %v, want %v", err, nil)
				}
				t.Logf("AsNoteStructured() = %v", string(show))
			} else {
				t.Fatal(err)
			}
		})
	}
}
