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

### filter
`./mgotools filter --help`

### info
`./mgotools info --help`
