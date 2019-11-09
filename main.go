package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/stkrizh/otus-go-memcload/appsinstalled"
)

const (
	// MemcacheInsertMaxAttempts defines how many attempts would be to
	// insert a record to Memcached
	MemcacheInsertMaxAttempts = 5
	// MemcacheInsertAttemptDelay defines delay between insertion attempts
	MemcacheInsertAttemptDelay = 200 * time.Millisecond
)

// Record represents one data parsed from one line of a log file.
type Record struct {
	Type string
	ID   string
	Lat  float64
	Lon  float64
	Apps []uint32
}

// Save record to Memcached
func (record *Record) Save(client *memcache.Client, dry bool) {
	recordProto := &appsinstalled.UserApps{
		Lon:  &record.Lon,
		Lat:  &record.Lat,
		Apps: record.Apps,
	}

	key := fmt.Sprintf("%s:%s", record.Type, record.ID)

	if dry {
		messageText := proto.MarshalTextString(recordProto)
		log.Infof("%s -> %s", key, strings.Replace(messageText, "\n", " ", -1))
		return
	}

	message, err := proto.Marshal(recordProto)
	if err != nil {
		log.Warnln("Could not serialize record:", record)
		return
	}

	item := memcache.Item{Key: key, Value: message}
	for attempt := 0; attempt < MemcacheInsertMaxAttempts; attempt++ {
		err := client.Set(&item)
		if err != nil {
			time.Sleep(MemcacheInsertAttemptDelay)
			continue
		}
		return
	}
}

// ParseRecord parses a new Record from raw row that must contain
// 5 parts separated by tabs.
func ParseRecord(row string) (Record, error) {
	var record Record

	parts := strings.Split(row, "\t")
	if len(parts) != 5 {
		return record, errors.New("Encountered invalid line")
	}

	record.Type = parts[0]
	record.ID = parts[1]

	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return record, errors.New("Encountered invalid `Lat`")
	}
	record.Lat = lat

	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return record, errors.New("Encountered invalid `Lon`")
	}
	record.Lon = lon

	rawApps := strings.Split(parts[4], ",")
	record.Apps = make([]uint32, len(rawApps))
	for ix, rawApp := range rawApps {
		app, err := strconv.ParseUint(rawApp, 10, 32)
		if err != nil {
			continue
		}
		record.Apps[ix] = uint32(app)
	}

	return record, nil
}

// ProcessLogFile reads file specified by `path` argument and
// processes each row of this file
func ProcessLogFile(client *memcache.Client, dry bool, path string) {
	file, err := os.Open(path)

	if err != nil {
		log.Fatal(err)
	}

	gz, err := gzip.NewReader(file)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		row := scanner.Text()
		record, err := ParseRecord(row)
		if err != nil {
			log.Warnf("%s for: %s", err, row)
			continue
		}
		record.Save(client, dry)
	}
}

func worker(id int, jobs chan int, results []chan int) {
	times := [5]int{8, 3, 6, 2, 5}

	for j := range jobs {
		log.Debugln("Worker", id, "started  job", j)
		time.Sleep(time.Second * time.Duration(times[j]))
		log.Debugln("Worker", id, "finished job", j)
		results[j] <- j * 2
	}
}

func main() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	// working_directory := "/home/sk/Dev/files/"

	// nJobs := 5
	// jobs := make(chan int)

	// results := make([]chan int, nJobs)
	// for i := 0; i < nJobs; i++ {
	// 	results[i] = make(chan int)
	// }

	// log.Debugln("Starting workers...")
	// for i := 0; i < nJobs; i++ {
	// 	go worker(i+1, jobs, results)
	// 	jobs <- i
	// }
	// log.Debugln("Workers have been started successfully")
	// close(jobs)

	// log.Debugln("Waiting for results...")
	// for i := 0; i < nJobs; i++ {
	// 	log.Debugf("Got: %d", <-results[i])
	// }
	mc := memcache.New("127.0.0.1:33016")
	ProcessLogFile(mc, false, "/home/sk/Dev/files/test20180404000000.tsv.gz")
}
