package main

import (
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/stkrizh/otus-go-memcload/appsinstalled"
)

func main() {
	fmt.Println("It works!")

	test := &appsinstalled.UserApps{
		Lon:  proto.Float64(42.345),
		Lat:  proto.Float64(33.5677),
		Apps: []uint32{1, 2, 3},
	}

	data, err := proto.Marshal(test)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	newTest := &appsinstalled.UserApps{}
	err = proto.Unmarshal(data, newTest)
	if err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

}
