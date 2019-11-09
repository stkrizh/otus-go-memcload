package main

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stkrizh/otus-go-memcload/appsinstalled"
)

type protoTestcase struct {
	lat  float64
	lon  float64
	apps []uint32
}

var protoCases = []protoTestcase{
	{42.345, 33.5677, []uint32{1, 2, 3}},
	{-42.345, 0.0, []uint32{100, 200, 300}},
	{-0.0, 100, nil},
	{0.0, 0.0, nil},
}

func areSlicesEqual(a, b []uint32) bool {

	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func TestProto(t *testing.T) {

	for _, tcase := range protoCases {

		test := &appsinstalled.UserApps{
			Lon:  &tcase.lon,
			Lat:  &tcase.lat,
			Apps: tcase.apps,
		}

		data, err := proto.Marshal(test)
		if err != nil {
			t.Error("marshaling error: ", err)
		}

		newTest := &appsinstalled.UserApps{}
		err = proto.Unmarshal(data, newTest)
		if err != nil {
			t.Error("unmarshaling error: ", err)
		}

		if *test.Lon != *newTest.Lon {
			t.Error("Lon-s are not equal: ", test.Lon, newTest.Lon)
		}

		if *test.Lat != *newTest.Lat {
			t.Error("Lat-s are not equal: ", test.Lat, newTest.Lat)
		}

		if !areSlicesEqual(test.Apps, newTest.Apps) {
			t.Error("Apps-s are not equal: ", test.Apps, newTest.Apps)
		}

	}

}
