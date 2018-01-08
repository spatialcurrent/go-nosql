package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

import (
	"github.com/mattn/go-colorable"
	"github.com/sirupsen/logrus"
	"github.com/ttacon/chalk"
	"gopkg.in/yaml.v2"
)

import (
	"github.com/spatialcurrent/go-collector/collector"
	"github.com/spatialcurrent/go-nosql/nosql"
)

var NOSQL_IMPORTER_VERSION = "0.0.1"

func main() {

	start := time.Now()

	var backend_type string
	var AWSDefaultRegion string
	var AWSAccessKeyId string
	var AWSSecretAccessKey string
	var DynamoDBUrl string
	var basepath string
	var table_name string
	var recursive bool
	var verbose bool
	var dry_run bool
	var version bool
	var help bool

	flag.StringVar(&backend_type, "backend", "dynamodb", "NoSQL backend type: dynamodb or mongodb.")
	flag.StringVar(&AWSDefaultRegion, "aws_default_region", os.Getenv("AWS_DEFAULT_REGION"), "Defaults to value of environment variable AWS_DEFAULT_REGION.")
	flag.StringVar(&AWSAccessKeyId, "aws_access_key_id", os.Getenv("AWS_ACCESS_KEY_ID"), "Defaults to value of environment variable AWS_ACCESS_KEY_ID")
	flag.StringVar(&AWSSecretAccessKey, "aws_secret_access_key", os.Getenv("AWS_SECRET_ACCESS_KEY"), "Defaults to value of environment variable AWS_SECRET_ACCESS_KEY.")
	flag.StringVar(&DynamoDBUrl, "dynamodb_url", "", "Defaults to AWS instance.")
	flag.StringVar(&basepath, "basepath", "", "Path to folder containing objects to import.")
	flag.StringVar(&table_name, "table", "", "Table name.")
	flag.BoolVar(&recursive, "recursive", false, "Recursive search within folder.")
	flag.BoolVar(&verbose, "verbose", false, "Provide verbose output")
	flag.BoolVar(&dry_run, "dry_run", false, "Connect to destination, but don't import any data.")
	flag.BoolVar(&version, "version", false, "Version")
	flag.BoolVar(&help, "help", false, "Print help")

	flag.Parse()

	if help {
		fmt.Println("Usage: nosql-importer -basepath BASEPATH -table TABLE_NAME [-recursive]")
		flag.PrintDefaults()
		os.Exit(0)
	} else if len(os.Args) == 1 {
		fmt.Println("Error: Provided no arguments.")
		fmt.Println("Run \"nosql-importer --help\" for more information.")
		os.Exit(0)
	} else if flag.NArg() > 0 {
		fmt.Println("Error: Provided extra command line arguments:", strings.Join(flag.Args(), ", "))
		fmt.Println("Run \"nosql-importer --help\" for more information.")
		os.Exit(0)
	}

	if version {
		fmt.Println(NOSQL_IMPORTER_VERSION)
		os.Exit(0)
	}

	logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true})
	logrus.SetOutput(colorable.NewColorableStdout())

	var log = logrus.New()

	if verbose {
		log.Println(chalk.Green, "Collecting filepaths from ", basepath, ".", chalk.Reset)
	}

	filepaths, err := collector.CollectFilepaths(basepath, []string{"json", "yaml", "yml"}, false, []string{})
	if err != nil {
		log.Println(chalk.Red, err, chalk.Reset)
		os.Exit(1)
	}

	if verbose {
		log.Println(chalk.Green, "Objects:", filepaths, chalk.Reset)
	}

	if dry_run {
		os.Exit(1)
	}

	if len(table_name) == 0 {
		log.Println(chalk.Red, "Missing table name.  Use -table on command line.", chalk.Reset)
		os.Exit(1)
	}

	backend, err := nosql.ConnectToBackend(backend_type, map[string]string{
		"AWSDefaultRegion":   AWSDefaultRegion,
		"AWSAccessKeyId":     AWSAccessKeyId,
		"AWSSecretAccessKey": AWSSecretAccessKey,
		"DynamoDBUrl":        DynamoDBUrl,
	})
	if err != nil {
		log.Println(chalk.Red, err, chalk.Reset)
		os.Exit(1)
	}

	if len(filepaths) > 0 {
		for _, f := range filepaths {
			buf := make([]byte, 0)
			buf, err := ioutil.ReadFile(f)
			if err != nil {
				log.Println(chalk.Red, "Error: Could not open file for object from path ", f, ".", chalk.Reset)
				continue
			}

			newObject := map[string]interface{}{}

			if strings.HasSuffix(f, ".json") {
				err := json.Unmarshal(buf, &newObject)
				if err != nil {
					log.Println(chalk.Red, "Error: Could not unmashal JSON from object at path", f, ".", chalk.Reset)
					log.Println(chalk.Red, "Original Error:", err, chalk.Reset)
					continue
				}
			} else if strings.HasSuffix(f, ".yaml") || strings.HasSuffix(f, ".yml") {
				err := yaml.Unmarshal(buf, &newObject)
				if err != nil {
					log.Println(chalk.Red, "Error: Could not unmashal YAML from object at path", f, ".", chalk.Reset)
					log.Println(chalk.Red, "Original Error:", err, chalk.Reset)
					continue
				}
			}

			err = (*backend).InsertItem(table_name, &newObject)
			if err != nil {
				log.Println(chalk.Red, err, chalk.Reset)
			}

		}
	}

	elapsed := time.Since(start)
	log.Info("Done in " + elapsed.String())

}
