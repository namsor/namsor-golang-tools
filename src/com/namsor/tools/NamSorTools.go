package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"hash"

	namsorapi "github.com/namsor/namsor-golang-sdk2"
	"golang.org/x/net/context"
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
	auth                context.Context
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

	if apiKey != "" {
		tools.auth = context.WithValue(context.Background(), namsorapi.ContextAPIKey, namsorapi.APIKey{
			Key: apiKey,
		})
	} else {
		print("Error! No API Key Provided!")
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
	flag.StringVar(&apiKey, "apiKey", "", "NamSor API Key")
	flag.StringVar(&inputFile, "i", "", "(short-hand) input file name")
	flag.StringVar(&inputFile, "inputFile", "", "input file name")
	flag.StringVar(&outputFile, "o", "", "(short-hand) output file name")
	flag.StringVar(&outputFile, "outputFile", "", "output file name")
	flag.BoolVar(&overwrite, "w", false, "(short-hand) overwrite existing output file")
	flag.BoolVar(&overwrite, "overwrite", false, "overwrite existing output file")
	flag.BoolVar(&recover, "r", false, "(short-hand) continue from a job (requires uid)")
	flag.BoolVar(&recover, "recover", false, "continue from a job (requires uid)")
	flag.StringVar(&inputDataFormat, "f", "", "(short-hand) input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
	flag.StringVar(&inputDataFormat, "inputDataFormat", "", "input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
	flag.BoolVar(&header, "header", false, "output header")
	flag.BoolVar(&uid, "uid", false, "input data has an ID prefix")
	flag.BoolVar(&digest, "digest", false, "SHA-256 digest names in output")
	flag.StringVar(&endpoint, "service", "", "(short-hand) service : parse / gender / origin / diaspora / usraceethnicity")
	flag.StringVar(&endpoint, "endpoint", "", "service : parse / gender / origin / diaspora / usraceethnicity")
	flag.StringVar(&encoding, "e", "", "(short-hand) encoding : UTF-8 by default")
	flag.StringVar(&encoding, "encoding", "", "encoding : UTF-8 by default")

	flag.Parse()

	tools := NewNamSorTools()
	print(tools.commandLineOptions["recover"].(bool))
}
