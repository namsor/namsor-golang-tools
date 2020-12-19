package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"github.com/antihax/optional"
	namsorapi "github.com/namsor/namsor-golang-sdk2"
	"golang.org/x/net/context"
	"hash"
	"reflect"
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
	socialApi           *namsorapi.SocialApiService
	TIMEOUT             int
	withUID             bool
	recover             bool
	skipErrors          bool
	digest              hash.Hash
	commandLineOptions  map[string]interface{}
	firstLastNamesGeoIn map[string]namsorapi.FirstLastNameGeoIn
	firstLastNamesIn    map[string]namsorapi.FirstLastNameIn
	personalNamesIn     map[string]namsorapi.PersonalNameIn
	personalNamesGeoIn  map[string]namsorapi.PersonalNameGeoIn
}

func NewNamSorTools() *NamrSorTools {
	config := namsorapi.NewConfiguration()
	client := namsorapi.NewAPIClient(config)
	tools := &NamrSorTools{
		separatorIn:         "|",
		separatorOut:        "|",
		adminApi:            client.AdminApi,
		personalApi:         client.PersonalApi,
		socialApi:           client.SocialApi,
		TIMEOUT:             30000,
		digest:              nil,
		recover:             recover,
		firstLastNamesGeoIn: map[string]namsorapi.FirstLastNameGeoIn{},
		firstLastNamesIn:    map[string]namsorapi.FirstLastNameIn{},
		personalNamesIn:     map[string]namsorapi.PersonalNameIn{},
		personalNamesGeoIn:  map[string]namsorapi.PersonalNameGeoIn{},
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

/*
	API Calls
*/
func (tools *NamrSorTools) processDiaspora(names []namsorapi.FirstLastNameGeoIn) (map[string]namsorapi.FirstLastNameDiasporaedOut, error) {
	result := map[string]namsorapi.FirstLastNameDiasporaedOut{}
	data := namsorapi.BatchFirstLastNameGeoIn{names}
	body := namsorapi.DiasporaBatchOpts{
		BatchFirstLastNameGeoIn: optional.NewInterface(data),
	}
	origined, _, err := tools.personalApi.DiasporaBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range origined.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processOrigin(names []namsorapi.FirstLastNameIn) (map[string]namsorapi.FirstLastNameOriginedOut, error) {
	result := map[string]namsorapi.FirstLastNameOriginedOut{}
	data := namsorapi.BatchFirstLastNameIn{
		names,
	}
	body := namsorapi.OriginBatchOpts{
		BatchFirstLastNameIn: optional.NewInterface(data),
	}
	origined, _, err := tools.personalApi.OriginBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range origined.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processOriginGeo(names []namsorapi.FirstLastNameGeoIn) (map[string]namsorapi.FirstLastNameOriginedOut, error) {
	var namesNoGeo []namsorapi.FirstLastNameIn
	for _, name := range names {
		nameNoGeo := namsorapi.FirstLastNameIn{
			name.Id,
			name.FirstName,
			name.LastName,
		}
		namesNoGeo = append(namesNoGeo, nameNoGeo)
	}

	return tools.processOrigin(namesNoGeo)
}

func (tools *NamrSorTools) processGender(names []namsorapi.FirstLastNameIn) (map[string]namsorapi.FirstLastNameGenderedOut, error) {
	result := map[string]namsorapi.FirstLastNameGenderedOut{}
	data := namsorapi.BatchFirstLastNameIn{
		names,
	}
	body := namsorapi.GenderBatchOpts{
		BatchFirstLastNameIn: optional.NewInterface(data),
	}
	gendered, _, err := tools.personalApi.GenderBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range gendered.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processGenderFull(names []namsorapi.PersonalNameIn) (map[string]namsorapi.PersonalNameGenderedOut, error) {
	result := map[string]namsorapi.PersonalNameGenderedOut{}
	data := namsorapi.BatchPersonalNameIn{
		names,
	}
	body := namsorapi.GenderFullBatchOpts{
		BatchPersonalNameIn: optional.NewInterface(data),
	}
	gendered, _, err := tools.personalApi.GenderFullBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range gendered.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processGenderGeo(names []namsorapi.FirstLastNameGeoIn) (map[string]namsorapi.FirstLastNameGenderedOut, error) {
	result := map[string]namsorapi.FirstLastNameGenderedOut{}
	data := namsorapi.BatchFirstLastNameGeoIn{
		names,
	}
	body := namsorapi.GenderGeoBatchOpts{
		BatchFirstLastNameGeoIn: optional.NewInterface(data),
	}
	gendered, _, err := tools.personalApi.GenderGeoBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range gendered.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processGenderFullGeo(names []namsorapi.PersonalNameGeoIn) (map[string]namsorapi.PersonalNameGenderedOut, error) {
	result := map[string]namsorapi.PersonalNameGenderedOut{}
	data := namsorapi.BatchPersonalNameGeoIn{
		names,
	}
	body := namsorapi.GenderFullGeoBatchOpts{
		BatchPersonalNameGeoIn: optional.NewInterface(data),
	}
	gendered, _, err := tools.personalApi.GenderFullGeoBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range gendered.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processCountry(names []namsorapi.PersonalNameIn) (map[string]namsorapi.PersonalNameGeoOut, error) {
	result := map[string]namsorapi.PersonalNameGeoOut{}
	data := namsorapi.BatchPersonalNameIn{
		names,
	}
	body := namsorapi.CountryBatchOpts{
		BatchPersonalNameIn: optional.NewInterface(data),
	}
	countried, _, err := tools.personalApi.CountryBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range countried.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processCountryAdapted(names_ []namsorapi.FirstLastNameIn) (map[string]namsorapi.PersonalNameGeoOut, error) {
	var names []namsorapi.PersonalNameIn
	for _, name := range names_ {
		adapted := namsorapi.PersonalNameIn{
			name.Id,
			name.FirstName + " " + name.LastName,
		}
		names = append(names, adapted)
	}

	return tools.processCountry(names)
}

func (tools *NamrSorTools) processParse(names []namsorapi.PersonalNameIn) (map[string]namsorapi.PersonalNameParsedOut, error) {
	result := map[string]namsorapi.PersonalNameParsedOut{}
	data := namsorapi.BatchPersonalNameIn{
		names,
	}
	body := namsorapi.ParseNameBatchOpts{
		BatchPersonalNameIn: optional.NewInterface(data),
	}
	parsed, _, err := tools.personalApi.ParseNameBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range parsed.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processParseGeo(names []namsorapi.PersonalNameGeoIn) (map[string]namsorapi.PersonalNameParsedOut, error) {
	result := map[string]namsorapi.PersonalNameParsedOut{}
	data := namsorapi.BatchPersonalNameGeoIn{
		names,
	}
	body := namsorapi.ParseNameGeoBatchOpts{
		BatchPersonalNameGeoIn: optional.NewInterface(data),
	}
	parsed, _, err := tools.personalApi.ParseNameGeoBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range parsed.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processUSRaceEthnicity(names []namsorapi.FirstLastNameGeoIn) (map[string]namsorapi.FirstLastNameUsRaceEthnicityOut, error) {
	result := map[string]namsorapi.FirstLastNameUsRaceEthnicityOut{}
	data := namsorapi.BatchFirstLastNameGeoIn{
		names,
	}
	body := namsorapi.UsRaceEthnicityBatchOpts{
		BatchFirstLastNameGeoIn: optional.NewInterface(data),
	}
	racedEthnicized, _, err := tools.personalApi.UsRaceEthnicityBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range racedEthnicized.PersonalNames {
		result[personalName.Id] = personalName
	}
	return result, nil
}

func (tools *NamrSorTools) processPhoneCode(names []namsorapi.FirstLastNamePhoneNumberIn) (map[string]namsorapi.FirstLastNamePhoneCodedOut, error) {
	result := map[string]namsorapi.FirstLastNamePhoneCodedOut{}
	data := namsorapi.BatchFirstLastNamePhoneNumberIn{
		names,
	}
	body := namsorapi.PhoneCodeBatchOpts{
		BatchFirstLastNamePhoneNumberIn: optional.NewInterface(data),
	}
	phoneCoded, _, err := tools.socialApi.PhoneCodeBatch(tools.auth, &body)
	if err != nil {
		return nil, err
	}
	for _, personalName := range phoneCoded.PersonalNamesWithPhoneNumbers {
		result[personalName.Id] = personalName
	}
	return result, nil
}

/*
	API call processing
*/
func (tools *NamrSorTools) processData(service string, outputHeaders []string, writer *bufio.Writer, flushBuffers bool, softwareNameAndVersion string) {
	if flushBuffers && len(tools.firstLastNamesIn) != 0 || len(tools.firstLastNamesIn) >= BATCH_SIZE {
		values := []namsorapi.FirstLastNameIn{}
		for _, v := range tools.firstLastNamesIn {
			values = append(values, v)
		}
		if service == SERVICE_NAME_ORIGIN {
			origins, _ := tools.processOrigin(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesIn, origins, softwareNameAndVersion)
		} else if service == SERVICE_NAME_GENDER {
			genders, _ := tools.processGender(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesIn, genders, softwareNameAndVersion)
		} else if service == SERVICE_NAME_COUNTRY {
			countrieds, _ := tools.processCountryAdapted(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesIn, countrieds, softwareNameAndVersion)
		}
		tools.firstLastNamesIn = make(map[string]namsorapi.FirstLastNameIn)
	}
	if flushBuffers && len(tools.firstLastNamesGeoIn) != 0 || len(tools.firstLastNamesGeoIn) >= BATCH_SIZE {
		values := []namsorapi.FirstLastNameGeoIn{}
		for _, v := range tools.firstLastNamesGeoIn {
			values = append(values, v)
		}
		if service == (SERVICE_NAME_ORIGIN) {
			origins, _ := tools.processOriginGeo(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, origins, softwareNameAndVersion)
		} else if service == (SERVICE_NAME_GENDER) {
			genders, _ := tools.processGenderGeo(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, genders, softwareNameAndVersion)
		} else if service == (SERVICE_NAME_DIASPORA) {
			diasporas, _ := tools.processDiaspora(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, diasporas, softwareNameAndVersion)
		} else if service == (SERVICE_NAME_USRACEETHNICITY) {
			usRaceEthnicities, _ := tools.processUSRaceEthnicity(values)
			tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, usRaceEthnicities, softwareNameAndVersion)
		}
		tools.firstLastNamesGeoIn = make(map[string]namsorapi.FirstLastNameGeoIn)
	}
	if flushBuffers && len(tools.personalNamesIn) != 0 || len(tools.personalNamesIn) >= BATCH_SIZE {
		values := []namsorapi.PersonalNameIn{}
		for _, v := range tools.personalNamesIn {
			values = append(values, v)
		}
		if service == (SERVICE_NAME_PARSE) {
			parseds, _ := tools.processParse(values)
			tools.appendX(writer, outputHeaders, tools.personalNamesIn, parseds, softwareNameAndVersion)
		} else if service == (SERVICE_NAME_GENDER) {
			genders, _ := tools.processGenderFull(values)
			tools.appendX(writer, outputHeaders, tools.personalNamesIn, genders, softwareNameAndVersion)
		} else if service == (SERVICE_NAME_COUNTRY) {
			countrieds, _ := tools.processCountry(values)
			tools.appendX(writer, outputHeaders, tools.personalNamesIn, countrieds, softwareNameAndVersion)
		}
		tools.personalNamesIn = make(map[string]namsorapi.PersonalNameIn)
	}
}

func (tools *NamrSorTools) appendX(writer *bufio.Writer, outputHeaders []string, inp interface{}, inpType reflect.Type, output interface{}, outputType reflect.Type, softwareNameAndVersion string) {

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
