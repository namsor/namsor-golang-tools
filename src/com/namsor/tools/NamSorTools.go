package main

import (
	"flag"
)

var (
	apiKey          string
	inputFile       string
	countryIso2     string
	outputFile      string
	overwrite       bool
	recover         bool
	inputDataFormat string
	header          bool
	uid             bool
	digest          bool
	endpoint        string
	encoding        string
)

func main() {
	flag.StringVar(&apiKey, "apiKey", "", "NamSor API Key")
	flag.StringVar(&inputFile, "inputFile", "", "input file name")
	flag.StringVar(&countryIso2, "countryIso2", "", "countryIso2 default")
	flag.StringVar(&countryIso2, "outputFile", "", "output file name")
	flag.BoolVar(&overwrite, "overwrite", false, "overwrite existing output file")
	flag.BoolVar(&overwrite, "recover", false, "continue from a job (requires uid)")
	flag.StringVar(&inputDataFormat, "inputDataFormat", "", "input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
	flag.BoolVar(&overwrite, "header", false, "output header")
	flag.BoolVar(&uid, "uid", false, "input data has an ID prefix")
	flag.BoolVar(&digest, "digest", false, "SHA-256 digest names in output")
	flag.StringVar(&endpoint, "endpoint", "", "service : parse / gender / origin / diaspora / usraceethnicity")
	flag.StringVar(&encoding, "encoding", "", "encoding : UTF-8 by default")

	flag.Parse()

}
