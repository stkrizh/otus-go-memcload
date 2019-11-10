package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// Options for command line interface
type Options struct {
	Pattern                string
	IDFA, GAID, ADID, DVID string
	Dry, Debug             bool
}

// Job keeps data for processing with goroutines
type Job struct {
	Clients map[string]*memcache.Client
	File    string
	Dry     bool
	Index   int
}

// Record represents one data parsed from one line of a log file.
type Record struct {
	Type string
	ID   string
	Lat  float64
	Lon  float64
	Apps []uint32
}

// Insert record from a log file to Memcached
func (record *Record) Insert(clients map[string]*memcache.Client, dry bool) {
	recordProto := &appsinstalled.UserApps{
		Lon:  &record.Lon,
		Lat:  &record.Lat,
		Apps: record.Apps,
	}

	key := fmt.Sprintf("%s:%s", record.Type, record.ID)

	if dry {
		messageText := proto.MarshalTextString(recordProto)
		log.Debugf("%s -> %s\n", key, strings.Replace(messageText, "\n", " ", -1))
		return
	}

	message, err := proto.Marshal(recordProto)
	if err != nil {
		log.Warnln("Could not serialize record:", record)
		return
	}

	client, ok := clients[record.Type]
	if !ok {
		log.Warnln("Unexpected device type:", record.Type)
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
func ProcessLogFile(clients map[string]*memcache.Client, dry bool, path string) {
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

	log.Infof("Processing %s\n", path)

	scanner := bufio.NewScanner(gz)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		row := scanner.Text()
		record, err := ParseRecord(row)
		if err != nil {
			log.Warnf("%s for: %s", err, row)
			continue
		}
		record.Insert(clients, dry)
	}
	log.Infof("File %s has been processed successfully.\n", path)
}

func worker(jobs chan Job, results []chan string) {
	for job := range jobs {
		ProcessLogFile(job.Clients, job.Dry, job.File)
		results[job.Index] <- job.File
	}
}

func parseCommandLine() Options {
	var options Options

	flag.StringVar(&options.Pattern, "pattern", "", "Pattern for searching log files")
	flag.StringVar(&options.IDFA, "idfa", "127.0.0.1:33013", "")
	flag.StringVar(&options.GAID, "gaid", "127.0.0.1:33014", "")
	flag.StringVar(&options.ADID, "adid", "127.0.0.1:33015", "")
	flag.StringVar(&options.DVID, "dvid", "127.0.0.1:33016", "")
	flag.BoolVar(&options.Dry, "dry", false, "Dry run (without interaction with Memcached)")
	flag.BoolVar(&options.Debug, "debug", false, "Show debug messages")
	flag.Parse()

	if options.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	if options.Pattern == "" {
		log.Fatalf("Pattern for searching log files must be provided.")
	}

	return options
}

func main() {
	options := parseCommandLine()

	files, err := filepath.Glob(options.Pattern)
	if err != nil {
		log.Fatalf("No files found for pattern %s", options.Pattern)
	}
	sort.Strings(files)

	log.Infoln("Found:", len(files), "files")

	clients := make(map[string]*memcache.Client)
	clients["idfa"] = memcache.New(options.IDFA)
	clients["gaid"] = memcache.New(options.GAID)
	clients["adid"] = memcache.New(options.ADID)
	clients["dvid"] = memcache.New(options.DVID)

	nJobs := len(files)
	jobs := make(chan Job)

	results := make([]chan string, nJobs)
	for i := 0; i < nJobs; i++ {
		results[i] = make(chan string)
	}

	log.Debugln("Starting workers...")
	for i := 0; i < nJobs; i++ {
		go worker(jobs, results)
		jobs <- Job{clients, files[i], options.Dry, i}
	}
	log.Debugln("Workers have been started successfully")
	close(jobs)

	log.Debugln("Waiting for results...")
	for i := 0; i < nJobs; i++ {
		log.Infof("Got: %s\n", <-results[i])
	}
}
