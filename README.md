# otus-go-memcload
Go (golang) version of
[MemcLoad](https://github.com/stkrizh/otus/tree/master/memcload) Python script. Program parses log records from files (compressed with **gzip**) specified by a glob pattern. Parsed records are serialized with [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview) and saved into Memcached storage.

Each line in a log file is a set of 5 tab-separated values, for example:
```
dvid\t63d3\t137.449673958\t89.8917375112\t3553,2919,7602
adid\tad12\t160.372422867\t-49.1875693538\t7848,8357
...
device_type\tdevice_id\latitude\longitude\installed_app_ids
```

Complete example of a log file is *sample.tsv.gz*

# Requirements
* Go (golang)
* Memcached

# Installation
Make sure you have **Go** installation on your system 
(https://golang.org/doc/install)

Prepare working directory:
```bash
mkdir $HOME/otus-memcload && cd $HOME/otus-memcload
export GOPATH=$HOME/otus-memcload
```

Get package:
```bash
go get -v github.com/stkrizh/otus-go-memcload
```

Run tests:
```bash
go test github.com/stkrizh/otus-go-memcload
```

Install:
```bash
go install github.com/stkrizh/otus-go-memcload
```

# Examples

To get help:
```bash
./bin/otus-go-memcload --help
```

To process the sample log file in "dry" mode:
```
cp ./src/github.com/stkrizh/otus-go-memcload/sample.tsv.gz .
./bin/otus-go-memcload --pattern "./logs/*.tsv.gz" --dry --debug
```

To run Memcached:
```bash
memcached -l 0.0.0.0:33013,0.0.0.0:33014,0.0.0.0:33015,0.0.0.0:33016
```

To process the sample log file and save records from it to Memcached (make sure it's running):
```
cp ./src/github.com/stkrizh/otus-go-memcload/sample.tsv.gz .
./bin/otus-go-memcload --pattern "./logs/*.tsv.gz"
```

# Comparison with the implementation in Python
Original implementation in Python may be found [here](https://github.com/stkrizh/otus/tree/master/memcload)

Testing data: https://yadi.sk/d/IkzKT-vR_uOhrQ

Both implementations are tested with `time` command.

x | Dry | Memcached
--- | --- | ---
**Go** | 29,91s user 0,43s system 347% cpu 8,727 total | 29,86s user 20,57s system 199% cpu 25,226 total
**Python** | 58,15s user 0,51s system 264% cpu 22,141 total | 107,94s user 22,11s system 218% cpu 59,632 total
