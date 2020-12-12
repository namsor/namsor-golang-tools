package main

import (
	"crypto"
	"flag"
	namsorapi "github.com/namsor/namsor-golang-sdk2"
	"hash"
)

const DEFAULT_DIGEST_ALGO string = "MD5"
const BATCH_SIZE int = 100

const INPUT_DATA_FORMAT_FNLN string = "fnln"
const INPUT_DATA_FORMAT_FNLNGEO string = "fnlngeo"
const INPUT_DATA_FORMAT_FULLNAME string = "name"
const INPUT_DATA_FORMAT_FULLNAMEGEO string = "namegeo"

var INPUT_DATA_FORMAT = [4]string{
	INPUT_DATA_FORMAT_FNLN,
	INPUT_DATA_FORMAT_FNLNGEO,
	INPUT_DATA_FORMAT_FULLNAME,
	INPUT_DATA_FORMAT_FULLNAMEGEO,
}

var INPUT_DATA_FORMAT_HEADER = [4][]string{
	{"firstName", "lastName"},
	{"firstName", "lastName", "countryIso2"},
	{"fullName"},
	{"fullName", "countryIso2"},
}

const SERVICE_NAME_PARSE string = "parse"
const SERVICE_NAME_GENDER string = "gender"
const SERVICE_NAME_ORIGIN string = "origin"
const SERVICE_NAME_COUNTRY string = "country"
const SERVICE_NAME_DIASPORA string = "diaspora"
const SERVICE_NAME_USRACEETHNICITY string = "usraceethnicity"

var SERVICES = []string{
	SERVICE_NAME_PARSE,
	SERVICE_NAME_GENDER,
	SERVICE_NAME_ORIGIN,
	SERVICE_NAME_COUNTRY,
	SERVICE_NAME_DIASPORA,
	SERVICE_NAME_USRACEETHNICITY,
}

var OUTPUT_DATA_PARSE_HEADER = []string{
	"firstNameParsed",
	"lastNameParsed",
	"nameParserType",
	"nameParserTypeAlt",
	"nameParserTypeScore",
	"script",
}
var OUTPUT_DATA_GENDER_HEADER = []string{
	"likelyGender",
	"likelyGenderScore",
	"probabilityCalibrated",
	"genderScale",
	"script",
}

var OUTPUT_DATA_ORIGIN_HEADER = []string{
	"countryOrigin",
	"countryOriginAlt",
	"probabilityCalibrated",
	"probabilityCalibratedAlt",
	"countryOriginScore",
	"script",
}
var OUTPUT_DATA_COUNTRY_HEADER = []string{
	"country",
	"countryAlt",
	"probabilityCalibrated",
	"probabilityCalibratedAlt",
	"countryScore",
	"script",
}
var OUTPUT_DATA_DIASPORA_HEADER = []string{
	"ethnicity",
	"ethnicityAlt",
	"ethnicityScore",
	"script",
}
var OUTPUT_DATA_USRACEETHNICITY_HEADER = []string{
	"raceEthnicity",
	"raceEthnicityAlt",
	"probabilityCalibrated",
	"probabilityCalibratedAlt",
	"raceEthnicityScore",
	"script",
}
var OUTPUT_DATA_HEADERS = [][]string{
	OUTPUT_DATA_PARSE_HEADER,
	OUTPUT_DATA_GENDER_HEADER,
	OUTPUT_DATA_ORIGIN_HEADER,
	OUTPUT_DATA_COUNTRY_HEADER,
	OUTPUT_DATA_DIASPORA_HEADER,
	OUTPUT_DATA_USRACEETHNICITY_HEADER,
}

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

type NamrSorTools struct {
	done               []string
	separatorOut       string
	separatorIn        string
	personalApi        *namsorapi.PersonalApiService
	adminApi           *namsorapi.AdminApiService
	TIMEOUT            int
	withUID            bool
	recover            bool
	skipErrors         bool
	digest             hash.Hash
	commandLineOptions map[string]interface{}
}

func NewNamSorTools() *NamrSorTools {
	config := namsorapi.NewConfiguration()
	client := namsorapi.NewAPIClient(config)
	tools := &NamrSorTools{
		separatorIn:  "|",
		separatorOut: "|",
		adminApi:     client.AdminApi,
		personalApi:  client.PersonalApi,
		TIMEOUT:      30000,
		digest:       crypto.MD5.New(),
		commandLineOptions: map[string]interface{}{
			"apiKey":          apiKey,
			"inputFile":       inputFile,
			"countryIso2":     countryIso2,
			"outputFile":      outputFile,
			"overwrite":       overwrite,
			"recover":         recover,
			"inputDataFormat": inputDataFormat,
			"header":          header,
			"uid":             uid,
			"digest":          digest,
			"endpoint":        endpoint,
			"encoding":        encoding,
		},
	}

	return tools
}

func (tools *NamrSorTools) isWithUID() bool {
	return tools.withUID
}

func (tools *NamrSorTools) isRecover() bool {
	return tools.recover
}

func (tools *NamrSorTools) getDigest() hash.Hash {
	return tools.digest
}

func main() {
	flag.StringVar(&apiKey, "apiKey", "", "NamSor API Key")
	flag.StringVar(&inputFile, "inputFile", "", "input file name")
	flag.StringVar(&countryIso2, "countryIso2", "", "countryIso2 default")
	flag.StringVar(&outputFile, "outputFile", "", "output file name")
	flag.BoolVar(&overwrite, "overwrite", false, "overwrite existing output file")
	flag.BoolVar(&recover, "recover", false, "continue from a job (requires uid)")
	flag.StringVar(&inputDataFormat, "inputDataFormat", "", "input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
	flag.BoolVar(&header, "header", false, "output header")
	flag.BoolVar(&uid, "uid", false, "input data has an ID prefix")
	flag.BoolVar(&digest, "digest", false, "SHA-256 digest names in output")
	flag.StringVar(&endpoint, "endpoint", "", "service : parse / gender / origin / diaspora / usraceethnicity")
	flag.StringVar(&encoding, "encoding", "", "encoding : UTF-8 by default")

	flag.Parse()

	tools := NewNamSorTools()
	print(tools.commandLineOptions["recover"].(bool))
}
