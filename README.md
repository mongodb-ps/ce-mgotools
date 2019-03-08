# mgotools

This tool suite is the spiritual successor to
[mtools](https://github.com/rueckstiess/mtools), a series of python
libraries designed to quickly parse and summarize
[MongoDB](https://www.mongodb.com/) log files.

MongoDB log files are full of useful information but also complicated
to parse through quickly. These tools are designed to easily filter
through large files quickly. They're designed to be extendable so
other projects can incorporate the binaries.

## Usage
There is one binary currently generated. The plan is to build drop-in
binary replacements for mloginfo and mlogfilter. Until then,
sub-commands are available.

Each tool may take input from _stdin_ or log files.
```
> cat mongod.log | mgotools query
> mgotools query mongod.log
```

Additionally, some command line arguments may be passed multiple times to apply
to multiple log files. For example, `mgotools filter --from 2019-01-01 --from 2018-01-01 mongod1.log mongod2.log`

In this example, the first `from` argument applies to `mongod1.log` and the 
second `from` argument applies to `mongod2.log`.

### filter
`./mgotools filter --help`

### info
`./mgotools info --help`

### queries
`./mgotools query --help`

The `query` command aggregates the canonicalized version 

### connstats
`./mgotools connstats --help`

### restart
`./mgotools restart --help`

## Build
The build process should be straightforward. Running the following commands
should work on properly configured Go environments:
```bash
> cd $GOPATH/src
> git clone https://github.com/jtv4k/mgotools
> cd mgotools
> go get 
> go build 
```

A binary named `mgotools` will be generated that can be executed using `./mgotools`.
