package main

import (
	"crypto/md5"
	"encoding/hex"
	flag "github.com/ogier/pflag"
	"hash"

	namsorapi "github.com/namsor/namsor-golang-sdk2"
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
	done                []string
	separatorOut        string
	separatorIn         string
	personalApi         *namsorapi.PersonalApiService
	adminApi            *namsorapi.AdminApiService
	TIMEOUT             int
	withUID             bool
	recover             bool
	skipErrors          bool
	digest              hash.Hash
	commandLineOptions  map[string]interface{}
	firstLastNamesGeoIn map[string]string
	firstLastNamesIn    map[string]string
	personalNamesIn     map[string]string
	personalNamesGeoIn  map[string]string
}

func NewNamSorTools() *NamrSorTools {
	config := namsorapi.NewConfiguration()
	client := namsorapi.NewAPIClient(config)
	tools := &NamrSorTools{
		separatorIn:         "|",
		separatorOut:        "|",
		adminApi:            client.AdminApi,
		personalApi:         client.PersonalApi,
		TIMEOUT:             30000,
		digest:              nil,
		recover:             recover,
		firstLastNamesGeoIn: map[string]string{},
		firstLastNamesIn:    map[string]string{},
		personalNamesIn:     map[string]string{},
		personalNamesGeoIn:  map[string]string{},
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

	if digest {
		tools.digest = md5.New()
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

func (tools *NamrSorTools) getCommandLineOptions() map[string]interface{} {
	return tools.commandLineOptions
}

// equal to digest(string)
func (tools *NamrSorTools) digestText(inClear string) string {
	if tools.getDigest() == nil || inClear == "" {
		return inClear
	}
	tools.digest.Write([]byte(inClear))
	return hex.EncodeToString(tools.digest.Sum(nil))
}

func main() {
	flag.StringVarP(&apiKey, "apiKey","a", "", "NamSor API Key")
	flag.StringVarP(&inputFile, "inputFile","i", "", "input file name")
	flag.StringVarP(&outputFile, "outputFile","o", "", "output file name")
	flag.BoolVarP(&overwrite, "overwrite","w", false, "overwrite existing output file")
	flag.BoolVarP(&recover, "recover","r", false, "continue from a job (requires uid)")
	flag.StringVarP(&inputDataFormat, "inputDataFormat","f", "", "input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
	flag.BoolVarP(&header, "header","h", false, "output header")
	flag.BoolVarP(&uid, "uid","u", false, "input data has an ID prefix")
	flag.BoolVarP(&digest, "digest","d", false, "SHA-256 digest names in output")
	flag.StringVarP(&endpoint, "service","s", "", "service : parse / gender / origin / diaspora / usraceethnicity")
	flag.StringVarP(&encoding, "encoding","e", "", "encoding : UTF-8 by default")

	flag.Parse()

	tools := NewNamSorTools()
	print(tools.commandLineOptions["recover"].(bool))
}
