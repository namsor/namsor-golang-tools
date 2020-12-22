# namsor-golang-tools-v2
NamSor command line tools, to append gender, origin, diaspora or us 'race'/ethnicity to a CSV file. The CSV file should in UTF-8 encoding, pipe-| demimited. It can be very large. 

## Installation
- Clone the repository
```bash
git clone https://github.com/namsor/namsor-golang-tools-v2.git
```
- Navigate to the directory
```bash
cd namsor-golang-tools-v2
```
- Install the dependencies
```bash
go mod vendor
```

NB: we use Unix conventions for file paths, ex. samples/some_fnln.txt but on MS Windows that would be samples\some_fnln.txt

## Usage

```bash
usage: go run NamSorTools.go --apiKey <apiKey> [--countryIso2 <countryIso2>] [--digest]
              [-e <encoding>] -f <inputDataFormat> [--help] [--header] -i <inputFile>
              [-o <outputFile>] [-r] --service <service> [--uid] [-w]
   -a, --apiKey string            NamSor API Key
   -d, --digest                   SHA-256 digest names in output
   -e, --encoding string          encoding : UTF-8 by default
   -h, --header                   output header
   -f, --inputDataFormat string   input data format : first name, last name (fnln) / first name, last name, geo country iso2 (fnlngeo) / full name (name) / full name, geo country iso2 (namegeo) 
   -i, --inputFile string         input file name
   -o, --outputFile string        output file name
   -w, --overwrite                overwrite existing output file
   -r, --recover                  continue from a job (requires uid)
   -s, --service string           service : parse / gender / origin / diaspora / usraceethnicity
   -u, --uid                      input data has an ID prefix
```

## Examples

To append gender to a list of first and last names : John|Smith

```bash
go run NamSorTools.go --apiKey <yourAPIKey> -w --header -f fnln -i path/to/samples/some_fnln.txt --service gender
```

To append origin to a list of first and last names : John|Smith

```bash
go run NamSorTools.go --apiKey <yourAPIKey> -w --header -f fnln -i path/to/samples/some_fnln.txt --service origin
```

To parse names into first and last name components (John Smith or Smith, John -> John|Smith)

```bash
go run NamSorTools.go --apiKey <yourAPIKey> -w --header -f name -i path/to/samples/some_name.txt --service parse
```

The recommended input format is to specify a unique ID and a geographic context (if known) as a countryIso2 code. 

To append gender to a list of id, first and last names, geographic context : id12|John|Smith|US

```bash
go run NamSorTools.go --apiKey <yourAPIKey> -w --header --uid -f fnlngeo -i path/to/samples/some_idfnlngeo.txt --service gender
```
To parse name into first and last name components, a geographic context is recommended (esp. for Latam names) : id12|John Smith|US

```bash
go run NamSorTools.go --apiKey <yourAPIKey> -w --header --uid -f namegeo -i path/to/samples/some_idnamegeo.txt --service parse
```
On large input files with a unique ID, it is possible to recover from where the process crashed and append to the existint output file, for example :

```bash
go run NamSorTools.go --apiKey <yourAPIKey> -r --header --uid -f fnlngeo -i path/to/samples/some_idfnlngeo.txt --service gender
```
## Extra notes
You can find the sample files used for these examples, inside 'samples' directory under the same name

## Anonymizing output data
The -digest option will digest personal names in file outpus, using a non reversible MD-5 hash. For example, John Smith will become 6117323d2cabbc17d44c2b44587f682c.
Please note that this doesn't apply to the PARSE output. 
