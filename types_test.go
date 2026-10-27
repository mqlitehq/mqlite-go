package mqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEpochMillisecondsBeforeUnixEpoch(t *testing.T) {
	instant := time.Unix(-1, 999500000)
	got, err := epochMilliseconds(instant)
	if err != nil {
		t.Fatal(err)
	}
	if got != -1 {
		t.Fatalf("epochMilliseconds(%s) = %d, want -1", instant, got)
	}
	if roundTrip := timeFromEpochMilliseconds(got); !roundTrip.Equal(time.Unix(-1, 999000000)) {
		t.Fatalf("round trip = %s", roundTrip)
	}
}

func TestEpochMillisecondsOutsideUnixNanoRange(t *testing.T) {
	instant := time.Date(2300, time.January, 2, 3, 4, 5, 678000000, time.FixedZone("test", 9*60*60))
	got, err := epochMilliseconds(instant)
	if err != nil {
		t.Fatal(err)
	}
	want := instant.Unix()*1000 + int64(instant.Nanosecond())/int64(time.Millisecond)
	if got != want {
		t.Fatalf("epochMilliseconds(%s) = %d, want %d", instant, got, want)
	}
	if roundTrip := timeFromEpochMilliseconds(got); !roundTrip.Equal(instant) {
		t.Fatalf("round trip = %s, want instant equal to %s", roundTrip, instant)
	}
}

func TestScheduleRejectsEpochMillisecondOverflow(t *testing.T) {
	client, err := Open(context.Background(), "http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendOne(context.Background(), "jobs", OutMessage{Body: []byte("x")}, SendOpts{
		At: time.Unix(9223372036854776, 0),
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}
