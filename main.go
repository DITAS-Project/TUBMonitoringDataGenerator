/*
 * Copyright 2018 Information Systems Engineering, TU Berlin, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *                       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This is being developed for the DITAS Project: https://www.ditas-project.eu/
 */

package main

import (
	"flag"
	"time"

	"github.com/DITAS-Project/TUBMonitoringDataGenerator/generator"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

type Generator int

const (
	Random = Generator(iota)
	ViolationFree
	Timed
)

var (
	Build string
)

var logger = logrus.New()
var log *logrus.Entry

func init() {
	if Build == "" {
		Build = "Debug"
	}
	logger.Formatter = new(prefixed.TextFormatter)
	logger.SetLevel(logrus.DebugLevel)
	log = logger.WithFields(logrus.Fields{
		"prefix": "tub-gen",
		"build":  Build,
	})
}

func main() {
	viper.SetDefault("elastic", "http://localhost:9200")
	viper.SetDefault("elasticAuth", "")
	viper.SetDefault("exchangeAddress", "")
	viper.SetDefault("blueprint", "resources/concrete_blueprint_doctor.json")
	viper.SetDefault("VDCName", "tubvdc")
	viper.SetDefault("Events", 100)

	viper.SetDefault("wt", 10*time.Second)
	viper.SetDefault("pause", true)

	viper.SetDefault("gen", 1)

	flag.String("elastic", "http://bar:9200", "used to define the elasticURL")
	flag.String("elasticAuth", "", "used to define the elastic authentication")
	flag.String("exchange", "", "used to define the exchangeURL")
	flag.String("blueprint", "resources/concrete_blueprint_doctor.json", "the blueprint to use")
	flag.Int("events", 100, "number of events generated and added to the elasticserach, runs infinitly if value is negative")
	flag.Duration("wt", 10*time.Second, "mean waittime in sec between events")
	flag.Bool("pause", true, "pause betweenEventes")
	flag.String("VDCName", "tub-vdc", "VDCName to use")
	flag.Bool("verbose", false, "activate interal logging.")
	flag.Int("gen", 1, "sets the internal generator to use ")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	viper.RegisterAlias("ElasticSearchURL", "elastic")

	if viper.GetBool("verbose") {
		logger.SetLevel(logrus.DebugLevel)
		viper.Debug()
		log.Infof("elastic %s - %s ", viper.GetString("elastic"), viper.GetString("ElasticSearchURL"))
	}

	var mg generator.MeterGenerator
	switch Generator(viper.GetInt("gen")) {
	case Random:
		log.Info("using Random Generator")
		mg = generator.NewRandomMeterGenerator()
		break
	case ViolationFree:
		log.Info("using ViolationFree Generator")
		mg = generator.NewViolationfreeGenerator()
		break
	case Timed:
		log.Info("using Timed Violation Generator")
		mg = generator.NewTimedViolationGenerator(pflag.Args())
		break
	default:
		log.Info("using ViolationFree Generator")
		mg = generator.NewViolationfreeGenerator()
	}

	generator.SetLogger(logger)
	generator.SetLog(log)

	gen, err := generator.NewGenerator(mg)

	if err != nil {
		log.Fatalf("failed to start gen %s", err)
	}

	gen.Start()
}
