package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/antihax/optional"
	namsorapi "github.com/namsor/namsor-golang-sdk2"
	"github.com/paulrosania/go-charset/charset"
	logger "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"golang.org/x/net/context"
	"hash"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

const DEFAULT_DIGEST_ALGO string = "MD5"
const BATCH_SIZE int = 100

const INPUT_DATA_FORMAT_FNLN string = "fnln"
const INPUT_DATA_FORMAT_FNLNGEO string = "fnlngeo"
const INPUT_DATA_FORMAT_FULLNAME string = "name"
const INPUT_DATA_FORMAT_FULLNAMEGEO string = "namegeo"
const INPUT_DATA_FORMAT_FNLNPHONE string = "fnlnphone"

var INPUT_DATA_FORMAT = [5]string{
	INPUT_DATA_FORMAT_FNLN,
	INPUT_DATA_FORMAT_FNLNGEO,
	INPUT_DATA_FORMAT_FULLNAME,
	INPUT_DATA_FORMAT_FULLNAMEGEO,
	INPUT_DATA_FORMAT_FNLNPHONE,
}

var INPUT_DATA_FORMAT_HEADER = [5][]string{
	{"firstName", "lastName"},
	{"firstName", "lastName", "countryIso2"},
	{"fullName"},
	{"fullName", "countryIso2"},
	{"firstName", "lastName", "phone"},
}

const SERVICE_NAME_PARSE string = "parse"
const SERVICE_NAME_GENDER string = "gender"
const SERVICE_NAME_ORIGIN string = "origin"
const SERVICE_NAME_COUNTRY string = "country"
const SERVICE_NAME_DIASPORA string = "diaspora"
const SERVICE_NAME_PHONECODE string = "phonecode"
const SERVICE_NAME_USRACEETHNICITY string = "usraceethnicity"

var SERVICES = []string{
	SERVICE_NAME_PARSE,
	SERVICE_NAME_GENDER,
	SERVICE_NAME_ORIGIN,
	SERVICE_NAME_COUNTRY,
	SERVICE_NAME_DIASPORA,
	SERVICE_NAME_PHONECODE,
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
var OUTPUT_DATA_PHONECODE_HEADER = []string{
	"internationalPhoneNumberVerified",
	"phoneCountryIso2Verified",
	"phoneCountryCode",
	"phoneCountryCodeAlt",
	"phoneCountryIso2",
	"phoneCountryIso2Alt",
	"originCountryIso2",
	"originCountryIso2Alt",
	"verified",
	"score",
	"script",
}
var OUTPUT_DATA_HEADERS = [][]string{
	OUTPUT_DATA_PARSE_HEADER,
	OUTPUT_DATA_GENDER_HEADER,
	OUTPUT_DATA_ORIGIN_HEADER,
	OUTPUT_DATA_COUNTRY_HEADER,
	OUTPUT_DATA_DIASPORA_HEADER,
	OUTPUT_DATA_USRACEETHNICITY_HEADER,
	OUTPUT_DATA_PHONECODE_HEADER,
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
	service         string
	encoding        string
)

var uidGen int = 0
var rowId int = 0

type NamrSorTools struct {
	done                        []string
	separatorOut                string
	separatorIn                 string
	auth                        context.Context
	personalApi                 *namsorapi.PersonalApiService
	adminApi                    *namsorapi.AdminApiService
	socialApi                   *namsorapi.SocialApiService
	TIMEOUT                     int
	withUID                     bool
	recover                     bool
	skipErrors                  bool
	digest                      hash.Hash
	commandLineOptions          map[string]interface{}
	firstLastNamesGeoIn         map[string]namsorapi.FirstLastNameGeoIn
	firstLastNamesIn            map[string]namsorapi.FirstLastNameIn
	personalNamesIn             map[string]namsorapi.PersonalNameIn
	personalNamesGeoIn          map[string]namsorapi.PersonalNameGeoIn
	firstLastNamesPhoneNumberIn map[string]namsorapi.FirstLastNamePhoneNumberIn
}

func NewNamSorTools() *NamrSorTools {
	config := namsorapi.NewConfiguration()
	client := namsorapi.NewAPIClient(config)
	tools := &NamrSorTools{
		separatorIn:                 "|",
		separatorOut:                "|",
		adminApi:                    client.AdminApi,
		personalApi:                 client.PersonalApi,
		socialApi:                   client.SocialApi,
		TIMEOUT:                     30000,
		digest:                      nil,
		skipErrors:                  false,
		recover:                     recover,
		firstLastNamesGeoIn:         map[string]namsorapi.FirstLastNameGeoIn{},
		firstLastNamesIn:            map[string]namsorapi.FirstLastNameIn{},
		personalNamesIn:             map[string]namsorapi.PersonalNameIn{},
		personalNamesGeoIn:          map[string]namsorapi.PersonalNameGeoIn{},
		firstLastNamesPhoneNumberIn: map[string]namsorapi.FirstLastNamePhoneNumberIn{},
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
			"endpoint":        service,
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

/*
	Support functions
*/
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

/*
	Getters
*/
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

func (tools *NamrSorTools) run() error {
	if apiKey == "" {
		return errors.New("missing api-key")
	}
	softwareNameAndVersion, _, err := tools.adminApi.SoftwareVersion(context.Background())
	if err != nil {
		logger.Fatalf(err.Error())
		return errors.New(fmt.Sprintf("can't get api-version %s", err.Error()))
	}

	service := tools.commandLineOptions["service"].(string)
	inputFileName := tools.commandLineOptions["inputFile"].(string)
	if inputFileName == "" {
		return errors.New("missing input file")
	}
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		logger.Fatal(err.Error())
		return errors.New(err.Error())
	}

	outputFileName := tools.commandLineOptions["outputFile"].(string)
	if outputFileName == "" {
		outputFileName = inputFileName + "." + service
		if digest {
			outputFileName += ".digest"
		}
		outputFileName += ".namsor"
		logger.Info(fmt.Sprintf("Outputing to %s", outputFileName))
	}

	outputFileExists := false
	outputFileOverwrite := tools.commandLineOptions["overwrite"].(bool)
	if _, err := os.Stat(outputFileName); err == nil {
		if !outputFileOverwrite && !tools.isRecover() {
			return errors.New(fmt.Sprintf("OutputFile %s already exsists, user -r to recover and continue job", inputFileName))
		}
		outputFileExists = true
	}
	if outputFileOverwrite && tools.isRecover() {
		return errors.New(fmt.Sprintf("You can overwrite OR  recover to %s", outputFileName))
	}
	if tools.isRecover() && !tools.isWithUID() {
		return errors.New(fmt.Sprintf("You can't recover without a uid %s", outputFileName))
	}
	if encoding == "" {
		encoding = "UTF-8"
	}

	if tools.isRecover() && outputFileExists {
		logger.Infof("Recovering from existing %s", outputFileName)
		outFile, err := os.Open(outputFile)
		if err != nil {
			logger.Fatal(err.Error())
			return errors.New(err.Error())
		}
		r, err := charset.NewReader(encoding, io.Reader(outFile))
		if err != nil {
			logger.Fatal(err.Error())
			return errors.New(err.Error())
		}
		readerDone := bufio.NewReader(r)
		doneLine, err := readerDone.ReadString('\n')
		if err != nil {
			logger.Fatal(err.Error())
			return errors.New(err.Error())
		}
		line := 0
		length := -1

		for doneLine != "" {
			if doneLine[0] == '#' {
				existingData := strings.Split(doneLine, "\\|")
				if length < 0 {
					length = len(existingData)
				} else if length != len(existingData) {
					logger.Warnf("Line %d doneLine = %s len=%d!=%d", line, doneLine, len(existingData), length)
				}
				tools.done = append(tools.done, existingData[0])
			}

			doneLine, err = readerDone.ReadString('\n')
			if err != nil {
				logger.Fatal(err.Error())
				return errors.New(err.Error())
			}

			if line%100000 == 0 {
				logger.Infof("Loading from existing %s : %d", outputFileName, line)
			}
			line++
		}
		err = outFile.Close()
		if err != nil {
			return errors.New(err.Error())
		}
	}

	r, errR := charset.NewReader(encoding, io.Reader(inputFile))
	if errR != nil {
		logger.Fatal(errR.Error())
		return errors.New(errR.Error())
	}

	outFile, err := os.Open(outputFile)
	if err != nil {
		logger.Fatal(err.Error())
		return errors.New(err.Error())
	}
	w, errW := charset.NewWriter(encoding, outFile)
	if errW != nil {
		logger.Fatal(errW.Error())
		return errors.New(errW.Error())
	}

	reader := bufio.NewReader(r)
	writer := bufio.NewWriter(w)

	err = tools.process(service, reader, writer, softwareNameAndVersion.SoftwareNameAndVersion)
	if err != nil {
		return errors.New(err.Error())
	}
	err = outFile.Close()
	if err != nil {
		return errors.New(err.Error())
	}
	err = inputFile.Close()
	if err != nil {
		return errors.New(err.Error())
	}
	return nil
}

// equal to digest(string)
func (tools *NamrSorTools) digestText(inClear string) string {
	if tools.getDigest() == nil || inClear == "" {
		return inClear
	}
	tools.digest.Write([]byte(inClear))
	return hex.EncodeToString(tools.digest.Sum(nil))
}

func (tools *NamrSorTools) computeScriptFirst(someString string) string {
	for i := 1; i < len(someString); i++ {
		c := []rune(someString)[i]
		if unicode.In(c, unicode.Common) {
			continue
		}
		for name, table := range unicode.Categories {
			if unicode.Is(table, c) {
				return name
			}
		}
	}
	return ""
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
func (tools *NamrSorTools) processData(service string, outputHeaders []string, writer *bufio.Writer, flushBuffers bool, softwareNameAndVersion string) error {
	if flushBuffers && len(tools.firstLastNamesIn) != 0 || len(tools.firstLastNamesIn) >= BATCH_SIZE {
		var err error = nil
		inpType := reflect.TypeOf(namsorapi.FirstLastNameIn{})
		values := []namsorapi.FirstLastNameIn{}
		for _, v := range tools.firstLastNamesIn {
			values = append(values, v)
		}
		if service == SERVICE_NAME_ORIGIN {
			origins, _ := tools.processOrigin(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesIn, inpType, origins, reflect.TypeOf(namsorapi.FirstLastNameOriginedOut{}), softwareNameAndVersion)
		} else if service == SERVICE_NAME_GENDER {
			genders, _ := tools.processGender(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesIn, inpType, genders, reflect.TypeOf(namsorapi.FirstLastNameGenderedOut{}), softwareNameAndVersion)
		} else if service == SERVICE_NAME_COUNTRY {
			countrieds, _ := tools.processCountryAdapted(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesIn, inpType, countrieds, reflect.TypeOf(namsorapi.PersonalNameGeoOut{}), softwareNameAndVersion)
		}
		tools.firstLastNamesIn = make(map[string]namsorapi.FirstLastNameIn)
		if err != nil {
			return err
		}
	}
	if flushBuffers && len(tools.firstLastNamesGeoIn) != 0 || len(tools.firstLastNamesGeoIn) >= BATCH_SIZE {
		var err error = nil
		inpType := reflect.TypeOf(namsorapi.FirstLastNameGeoIn{})
		values := []namsorapi.FirstLastNameGeoIn{}
		for _, v := range tools.firstLastNamesGeoIn {
			values = append(values, v)
		}
		if service == (SERVICE_NAME_ORIGIN) {
			origins, _ := tools.processOriginGeo(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, inpType, origins, reflect.TypeOf(namsorapi.FirstLastNameOriginedOut{}), softwareNameAndVersion)
		} else if service == (SERVICE_NAME_GENDER) {
			genders, _ := tools.processGenderGeo(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, inpType, genders, reflect.TypeOf(namsorapi.FirstLastNameGenderedOut{}), softwareNameAndVersion)
		} else if service == (SERVICE_NAME_DIASPORA) {
			diasporas, _ := tools.processDiaspora(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, inpType, diasporas, reflect.TypeOf(namsorapi.FirstLastNameDiasporaedOut{}), softwareNameAndVersion)
		} else if service == (SERVICE_NAME_USRACEETHNICITY) {
			usRaceEthnicities, _ := tools.processUSRaceEthnicity(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesGeoIn, inpType, usRaceEthnicities, reflect.TypeOf(namsorapi.FirstLastNameUsRaceEthnicityOut{}), softwareNameAndVersion)
		}
		tools.firstLastNamesGeoIn = make(map[string]namsorapi.FirstLastNameGeoIn)
		if err != nil {
			return err
		}
	}
	if flushBuffers && len(tools.personalNamesIn) != 0 || len(tools.personalNamesIn) >= BATCH_SIZE {
		var err error = nil
		inpType := reflect.TypeOf(namsorapi.PersonalNameIn{})
		values := []namsorapi.PersonalNameIn{}
		for _, v := range tools.personalNamesIn {
			values = append(values, v)
		}
		if service == (SERVICE_NAME_PARSE) {
			parseds, _ := tools.processParse(values)
			err = tools.appendX(writer, outputHeaders, tools.personalNamesIn, inpType, parseds, reflect.TypeOf(namsorapi.PersonalNameParsedOut{}), softwareNameAndVersion)
		} else if service == (SERVICE_NAME_GENDER) {
			genders, _ := tools.processGenderFull(values)
			err = tools.appendX(writer, outputHeaders, tools.personalNamesIn, inpType, genders, reflect.TypeOf(namsorapi.PersonalNameGenderedOut{}), softwareNameAndVersion)
		} else if service == (SERVICE_NAME_COUNTRY) {
			countrieds, _ := tools.processCountry(values)
			err = tools.appendX(writer, outputHeaders, tools.personalNamesIn, inpType, countrieds, reflect.TypeOf(namsorapi.PersonalNameGeoOut{}), softwareNameAndVersion)
		}
		tools.personalNamesIn = make(map[string]namsorapi.PersonalNameIn)
		if err != nil {
			return err
		}
	}
	if flushBuffers && len(tools.personalNamesGeoIn) != 0 || len(tools.personalNamesGeoIn) >= BATCH_SIZE {
		var err error = nil
		inpType := reflect.TypeOf(namsorapi.PersonalNameGeoIn{})
		values := []namsorapi.PersonalNameGeoIn{}
		for _, v := range tools.personalNamesGeoIn {
			values = append(values, v)
		}
		if service == (SERVICE_NAME_PARSE) {
			parseds, _ := tools.processParseGeo(values)
			err = tools.appendX(writer, outputHeaders, tools.personalNamesGeoIn, inpType, parseds, reflect.TypeOf(namsorapi.PersonalNameParsedOut{}), softwareNameAndVersion)
		} else if service == (SERVICE_NAME_GENDER) {
			genders, _ := tools.processGenderFullGeo(values)
			err = tools.appendX(writer, outputHeaders, tools.personalNamesGeoIn, inpType, genders, reflect.TypeOf(namsorapi.PersonalNameGenderedOut{}), softwareNameAndVersion)
		}
		tools.personalNamesGeoIn = make(map[string]namsorapi.PersonalNameGeoIn)
		if err != nil {
			return err
		}
	}
	if flushBuffers && len(tools.firstLastNamesPhoneNumberIn) != 0 || len(tools.firstLastNamesPhoneNumberIn) >= BATCH_SIZE {
		var err error = nil
		inpType := reflect.TypeOf(namsorapi.FirstLastNamePhoneNumberIn{})
		values := []namsorapi.FirstLastNamePhoneNumberIn{}
		for _, v := range tools.firstLastNamesPhoneNumberIn {
			values = append(values, v)
		}
		if service == (SERVICE_NAME_PHONECODE) {
			phoneCodes, _ := tools.processPhoneCode(values)
			err = tools.appendX(writer, outputHeaders, tools.firstLastNamesPhoneNumberIn, inpType, phoneCodes, reflect.TypeOf(namsorapi.FirstLastNamePhoneCodedOut{}), softwareNameAndVersion)
		}
		tools.firstLastNamesPhoneNumberIn = make(map[string]namsorapi.FirstLastNamePhoneNumberIn)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
	Data processing
*/
func (tools *NamrSorTools) process(service string, reader *bufio.Reader, writer *bufio.Writer, softwareNameAndVersion string) error {
	var lineId = 0
	inputDataFormat = tools.getCommandLineOptions()["inputDataFormat"].(string)
	var inputHeaders []string = nil
	for i, val := range INPUT_DATA_FORMAT {
		if val == inputDataFormat {
			inputHeaders = INPUT_DATA_FORMAT_HEADER[i]
			break
		}
	}
	if inputHeaders == nil {
		return errors.New("Invalid inputFileFormat " + inputDataFormat)
	}
	var outputHeaders []string = nil
	for i, val := range SERVICES {
		if val == service {
			outputHeaders = OUTPUT_DATA_HEADERS[i]
			break
		}
	}
	if outputHeaders == nil {
		return errors.New("Invalid service " + service)
	}
	var appendHeader bool = tools.getCommandLineOptions()["header"].(bool)
	if appendHeader && !tools.isRecover() || (tools.isRecover() && len(tools.done) == 0) {
		// don't append a header to an existing file
		err := tools.appendHeader(writer, inputHeaders, outputHeaders)
		if err != nil {
			return err
		}
	}
	var dataLenExpected int = len(inputHeaders)
	if tools.withUID {
		dataLenExpected += 1
	}
	dataFormatExpected := ""
	if tools.isWithUID() {
		dataFormatExpected += "uid" + tools.separatorIn
	}
	countryIso2Default := tools.getCommandLineOptions()["countryIso2Default"].(string)

	for i, val := range inputHeaders {
		dataFormatExpected += val
		if i < len(inputHeaders)-1 {
			dataFormatExpected += tools.separatorIn
		}
	}

	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	for line != "" {
		if strings.HasPrefix(line, "#") {
			if strings.HasSuffix(line, "|") {
				line = line + " "
			}
			lineData := strings.Split(line, "|")
			if len(lineData) != dataLenExpected {
				if tools.skipErrors {
					log.Println("Line " + strconv.Itoa(lineId) + ", expected input with format : " + dataFormatExpected + " line = " + line) // todo change to new logger
					lineId++
					line, err = reader.ReadString('\n')
					if err != nil && err != io.EOF {
						return err
					}
					continue
				} else {
					return errors.New("Line " + strconv.Itoa(lineId) + ", expected input with format : " + dataFormatExpected + " line = " + line)
				}
			}
			var uId string = ""
			var col int = 0
			if tools.isWithUID() {
				uId = lineData[col]
				col += 1
			} else {
				uId = "uid" + strconv.Itoa(uidGen)
				uidGen += 1
			}
			if tools.isRecover() && contains(tools.done, uId) {
				// skip this, as it's already done
			} else {
				if inputDataFormat == (INPUT_DATA_FORMAT_FNLN) {
					firstName := lineData[col]
					col += 1
					lastName := lineData[col]
					col += 1
					firstLastNameIn := namsorapi.FirstLastNameIn{
						uId,
						firstName,
						lastName,
					}
					tools.firstLastNamesIn[uId] = firstLastNameIn
				} else if inputDataFormat == (INPUT_DATA_FORMAT_FNLNGEO) {
					firstName := lineData[col]
					col += 1
					lastName := lineData[col]
					col += 1
					countryIso2 := lineData[col]
					col += 1
					if (strings.Trim(countryIso2, " ") == "") && countryIso2Default != "" {
						countryIso2 = countryIso2Default
					}
					firstLastNameGeoIn := namsorapi.FirstLastNameGeoIn{
						uId,
						firstName,
						lastName,
						countryIso2,
					}
					tools.firstLastNamesGeoIn[uId] = firstLastNameGeoIn
				} else if inputDataFormat == (INPUT_DATA_FORMAT_FULLNAME) {
					fullName := lineData[col]
					col += 1
					personalNameIn := namsorapi.PersonalNameIn{
						uId,
						fullName,
					}
					tools.personalNamesIn[uId] = personalNameIn
				} else if inputDataFormat == (INPUT_DATA_FORMAT_FULLNAMEGEO) {
					fullName := lineData[col]
					col += 1
					countryIso2 := lineData[col]
					col += 1
					if (strings.Trim(countryIso2, " ") == "") && countryIso2Default != "" {
						countryIso2 = countryIso2Default
					}
					personalNameGeoIn := namsorapi.PersonalNameGeoIn{
						uId,
						fullName,
						countryIso2,
					}
					tools.personalNamesGeoIn[uId] = personalNameGeoIn
				} else if inputDataFormat == (INPUT_DATA_FORMAT_FNLNPHONE) {
					firstName := lineData[col]
					col += 1
					lastName := lineData[col]
					col += 1
					phoneNumber := lineData[col]
					col += 1
					firstLastNamePhoneNumberIn := namsorapi.FirstLastNamePhoneNumberIn{
						uId,
						firstName,
						lastName,
						phoneNumber,
						nil,
					}

					tools.firstLastNamesPhoneNumberIn[uId] = firstLastNamePhoneNumberIn
				}
				err := tools.processData(service, outputHeaders, writer, false, softwareNameAndVersion)
				if err != nil {
					return err
				}
			}
		}
		lineId += 1
		line, err = reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
	}
	err = tools.processData(service, outputHeaders, writer, true, softwareNameAndVersion)
	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (tools *NamrSorTools) appendHeader(writer *bufio.Writer, inputHeaders []string, outputHeaders []string) error {
	_, err := writer.WriteString("#uid" + tools.separatorOut)

	for _, inputHeader := range inputHeaders {
		_, err := writer.WriteString(inputHeader + tools.separatorOut)
		if err != nil {
			logger.Fatal(err.Error())
			return errors.New(err.Error())
		}
	}
	for _, outputHeader := range outputHeaders {
		_, err := writer.WriteString(outputHeader + tools.separatorOut)
		if err != nil {
			logger.Fatal(err.Error())
			return errors.New(err.Error())
		}
	}

	_, err = writer.WriteString("version" + tools.separatorOut)
	if err != nil {
		logger.Fatal(err.Error())
		return errors.New(err.Error())
	}

	_, err = writer.WriteString("rowId" + "\n")
	if err != nil {
		logger.Fatal(err.Error())
		return errors.New(err.Error())
	}

	err = writer.Flush()
	if err != nil {
		return errors.New(err.Error())
	}
	return nil
}

func (tools *NamrSorTools) appendX(writer *bufio.Writer, outputHeaders []string, inp interface{}, inpType reflect.Type, output interface{}, outputType reflect.Type, softwareNameAndVersion string) error {
	flushedUID := make(map[string]bool) // Used as a set
	inputMap := reflect.ValueOf(inp)
	outputMap := reflect.ValueOf(output)
	if inputMap.Kind() == reflect.Map && outputMap.Kind() == reflect.Map {
		separatorOut := tools.separatorOut
		for _, key := range inputMap.MapKeys() {
			uid := key.Interface().(string)
			flushedUID[uid] = true
			_, err := writer.WriteString(uid + separatorOut)
			if err != nil {
				logger.Fatal(err.Error())
				return errors.New(err.Error())
			}

			inputObject := inputMap.MapIndex(key)
			outputObject := outputMap.MapIndex(key)

			switch inpType {
			case reflect.TypeOf(namsorapi.FirstLastNameIn{}):
				firstLastNameIn := inputObject.Interface().(namsorapi.FirstLastNameIn)
				_, err = writer.WriteString(tools.digestText(firstLastNameIn.FirstName+separatorOut) + separatorOut + tools.digestText(firstLastNameIn.LastName) + separatorOut)
				if err != nil {
					logger.Fatal(err.Error())
					return errors.New(err.Error())
				}
			case reflect.TypeOf(namsorapi.FirstLastNameGeoIn{}):
				firstLastNameGeoIn := inputObject.Interface().(namsorapi.FirstLastNameGeoIn)
				_, err = writer.WriteString(tools.digestText(firstLastNameGeoIn.FirstName+separatorOut) + separatorOut + tools.digestText(firstLastNameGeoIn.LastName) + separatorOut + firstLastNameGeoIn.CountryIso2 + separatorOut)
				if err != nil {
					logger.Fatal(err.Error())
					return errors.New(err.Error())
				}
			case reflect.TypeOf(namsorapi.PersonalNameIn{}):
				personalNameIn := inputObject.Interface().(namsorapi.PersonalNameIn)
				_, err = writer.WriteString(tools.digestText(personalNameIn.Name + separatorOut))
				if err != nil {
					logger.Fatal(err.Error())
					return errors.New(err.Error())
				}
			case reflect.TypeOf(namsorapi.PersonalNameGeoIn{}):
				personalNameGeoIn := inputObject.Interface().(namsorapi.PersonalNameGeoIn)
				_, err = writer.WriteString(tools.digestText(personalNameGeoIn.Name + separatorOut + personalNameGeoIn.CountryIso2 + separatorOut))
				if err != nil {
					logger.Fatal(err.Error())
					return errors.New(err.Error())
				}
			case reflect.TypeOf(namsorapi.FirstLastNamePhoneNumberIn{}):
				firstLastNamePhoneNumberIn := inputObject.Interface().(namsorapi.FirstLastNamePhoneNumberIn)
				_, err = writer.WriteString(tools.digestText(firstLastNamePhoneNumberIn.FirstName + separatorOut + firstLastNamePhoneNumberIn.LastName + separatorOut + firstLastNamePhoneNumberIn.PhoneNumber + separatorOut))
				if err != nil {
					logger.Fatal(err.Error())
					return errors.New(err.Error())
				}
			default:
				logger.Fatal("Invalid input type")
				return errors.New(fmt.Sprintf("Invalid input type : %s ", inpType.Name()))
			}

			if output == nil {
				for i := 0; i < len(outputHeaders); i++ {
					_, err = writer.WriteString("" + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				}
			} else {

				switch outputType {
				case reflect.TypeOf(namsorapi.FirstLastNameGenderedOut{}):
					firstLastNameGenderedOut := outputObject.Interface().(namsorapi.FirstLastNameGenderedOut)
					scriptName := tools.computeScriptFirst(firstLastNameGenderedOut.LastName)
					_, err = writer.WriteString(firstLastNameGenderedOut.LikelyGender + separatorOut +
						fmt.Sprintf("%f", firstLastNameGenderedOut.Score) + separatorOut +
						fmt.Sprintf("%f", firstLastNameGenderedOut.ProbabilityCalibrated) + separatorOut +
						fmt.Sprintf("%f", firstLastNameGenderedOut.GenderScale) + separatorOut +
						scriptName + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.FirstLastNameOriginedOut{}):
					firstLastNameOriginedOut := outputObject.Interface().(namsorapi.FirstLastNameOriginedOut)
					scriptName := tools.computeScriptFirst(firstLastNameOriginedOut.LastName)
					_, err = writer.WriteString(firstLastNameOriginedOut.CountryOrigin + separatorOut +
						firstLastNameOriginedOut.CountryOriginAlt + separatorOut +
						fmt.Sprintf("%f", firstLastNameOriginedOut.ProbabilityCalibrated) + separatorOut +
						fmt.Sprintf("%f", firstLastNameOriginedOut.ProbabilityAltCalibrated) + separatorOut +
						fmt.Sprintf("%f", firstLastNameOriginedOut.Score) + separatorOut +
						scriptName + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.FirstLastNameDiasporaedOut{}):
					firstLastNameDiasporaedOut := outputObject.Interface().(namsorapi.FirstLastNameDiasporaedOut)
					scriptName := tools.computeScriptFirst(firstLastNameDiasporaedOut.LastName)
					_, err = writer.WriteString(firstLastNameDiasporaedOut.Ethnicity + separatorOut +
						firstLastNameDiasporaedOut.EthnicityAlt + separatorOut +
						fmt.Sprintf("%f", firstLastNameDiasporaedOut.Score) + separatorOut +
						scriptName + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.FirstLastNameUsRaceEthnicityOut{}):
					firstLastNameUsRaceEthnicityOut := outputObject.Interface().(namsorapi.FirstLastNameUsRaceEthnicityOut)
					scriptName := tools.computeScriptFirst(firstLastNameUsRaceEthnicityOut.LastName)
					_, err = writer.WriteString(firstLastNameUsRaceEthnicityOut.RaceEthnicity + separatorOut +
						firstLastNameUsRaceEthnicityOut.RaceEthnicityAlt + separatorOut +
						fmt.Sprintf("%f", firstLastNameUsRaceEthnicityOut.ProbabilityCalibrated) + separatorOut +
						fmt.Sprintf("%f", firstLastNameUsRaceEthnicityOut.ProbabilityAltCalibrated) + separatorOut +
						fmt.Sprintf("%f", firstLastNameUsRaceEthnicityOut.Score) + separatorOut +
						scriptName + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.PersonalNameGenderedOut{}):
					personalNameGenderedOut := outputObject.Interface().(namsorapi.PersonalNameGenderedOut)
					scriptName := tools.computeScriptFirst(personalNameGenderedOut.Name)
					_, err = writer.WriteString(personalNameGenderedOut.LikelyGender + separatorOut +
						fmt.Sprintf("%f", personalNameGenderedOut.Score) + separatorOut +
						fmt.Sprintf("%f", personalNameGenderedOut.GenderScale) + separatorOut +
						scriptName + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.PersonalNameGeoOut{}):
					personalNameGeoOut := outputObject.Interface().(namsorapi.PersonalNameGeoOut)
					scriptName := tools.computeScriptFirst(personalNameGeoOut.Name)
					_, err = writer.WriteString(personalNameGeoOut.Country + separatorOut +
						personalNameGeoOut.CountryAlt + separatorOut +
						fmt.Sprintf("%f", personalNameGeoOut.ProbabilityCalibrated) + separatorOut +
						fmt.Sprintf("%f", personalNameGeoOut.ProbabilityAltCalibrated) + separatorOut +
						fmt.Sprintf("%f", personalNameGeoOut.Score) + separatorOut +
						scriptName + separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.PersonalNameParsedOut{}):
					personalNameParsedOut := outputObject.Interface().(namsorapi.PersonalNameParsedOut)
					firstNameParsed := personalNameParsedOut.FirstLastName.FirstName
					lastNameParsed := personalNameParsedOut.FirstLastName.LastName
					scriptName := tools.computeScriptFirst(personalNameParsedOut.Name)
					_, err = writer.WriteString(firstNameParsed + separatorOut +
						lastNameParsed + separatorOut +
						personalNameParsedOut.NameParserType + separatorOut +
						personalNameParsedOut.NameParserTypeAlt + separatorOut +
						fmt.Sprintf("%f", personalNameParsedOut.Score) + separatorOut +
						scriptName +
						separatorOut)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				case reflect.TypeOf(namsorapi.FirstLastNamePhoneCodedOut{}):
					firstLastNamePhoneCodedOut := outputObject.Interface().(namsorapi.FirstLastNamePhoneCodedOut)
					scriptName := tools.computeScriptFirst(firstLastNamePhoneCodedOut.LastName)
					_, err = writer.WriteString(firstLastNamePhoneCodedOut.InternationalPhoneNumberVerified + separatorOut +
						firstLastNamePhoneCodedOut.PhoneCountryIso2Verified + separatorOut +
						fmt.Sprintf("%d", firstLastNamePhoneCodedOut.PhoneCountryCode) + separatorOut +
						fmt.Sprintf("%d", firstLastNamePhoneCodedOut.PhoneCountryCodeAlt) + separatorOut +
						firstLastNamePhoneCodedOut.PhoneCountryIso2 + separatorOut +
						firstLastNamePhoneCodedOut.PhoneCountryIso2Alt + separatorOut +
						firstLastNamePhoneCodedOut.OriginCountryIso2 + separatorOut +
						firstLastNamePhoneCodedOut.OriginCountryIso2Alt + separatorOut +
						fmt.Sprintf("%t", firstLastNamePhoneCodedOut.Verified) + separatorOut +
						fmt.Sprintf("%f", firstLastNamePhoneCodedOut.Score) + separatorOut +
						scriptName)
					if err != nil {
						logger.Fatal(err.Error())
						return errors.New(err.Error())
					}
				default:
					return errors.New(fmt.Sprintf("Invalid output type : %s ", outputType.Name()))
				}
			}
			_, err = writer.WriteString(softwareNameAndVersion + separatorOut)
			_, err = writer.WriteString(fmt.Sprintf("%d\n", rowId))
			rowId++
		}
		err := writer.Flush()
		if err != nil {
			logger.Fatal(err.Error())
			return errors.New(err.Error())
		}
		if tools.isRecover() {
			keys := make([]string, 0, len(flushedUID))
			for k := range flushedUID {
				keys = append(keys, k)
			}
			tools.done = append(tools.done, keys...)
		}
		if rowId%100 == 0 && rowId < 1000 ||
			rowId%1000 == 0 && rowId < 10000 ||
			rowId%10000 == 0 && rowId < 100000 ||
			rowId%100000 == 0 {
			logger.Info(fmt.Sprintf("Processed %d rows.", rowId))
		}
	}
	return nil
}

func main() {
	flag.StringVarP(&apiKey, "apiKey", "a", "", "NamSor API Key")
	flag.StringVarP(&inputFile, "inputFile", "i", "", "(short-hand) input file name")
	flag.StringVarP(&outputFile, "outputFile", "w", "", "(short-hand) output file name")
	flag.BoolVarP(&overwrite, "overwrite", "o", false, "(short-hand) overwrite existing output file")
	flag.BoolVarP(&recover, "recover", "r", false, "(short-hand) continue from a job (requires uid)")
	flag.StringVarP(&inputDataFormat, "inputDataFormat", "f", "", "(short-hand) input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) ")
	flag.BoolVarP(&header, "header", "h", false, "output header")
	flag.BoolVarP(&uid, "uid", "u", false, "input data has an ID prefix")
	flag.BoolVarP(&digest, "digest", "d", false, "SHA-256 digest names in output")
	flag.StringVarP(&service, "service", "s", "", "(short-hand) service : parse / gender / origin / diaspora / usraceethnicity")
	flag.StringVarP(&encoding, "encoding", "e", "", "(short-hand) encoding : UTF-8 by default")

	flag.Parse()

	tools := NewNamSorTools()
	//print(tools.commandLineOptions["recover"].(bool))
	err := tools.run()
	if err != nil {
		logger.Fatalf(err.Error())
	}
}
