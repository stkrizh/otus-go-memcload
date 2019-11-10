# otus-go-memcload
Go (golang) version of
[MemcLoad](https://github.com/stkrizh/otus/tree/master/memcload) Python script. Program parses log records from files (compressed with **gzip**) specified by a glob pattern. Parsed records are serialized with [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview) and saved into Memcached storage.

Each line in of a log file is a set of 5 tab-separated values, for example:
```
dvid\t63d3\t137.449673958\t89.8917375112\t3553,2919,7602
adid\tad12\t160.372422867\t-49.1875693538\t7848,8357
...
```

Complete example of a log file is *sample.tsv.gz*

# Installation
Make sure you have **Go** installation on your system 
(https://golang.org/doc/install)

```bash
mkdir ./otus && cd ./otus
export GOPATH=.
go get github.com/stkrizh/otus-go-memcload
```
