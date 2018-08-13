package main

import (
	"flag"
	"time"

	"github.com/DITAS-Project/TUBMonitoringDataGenerator/generator"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
)

type Generator int

const (
	Random = Generator(iota)
	ViolationFree
	Timed
)

func main() {
	viper.SetDefault("ElasticSearchURL", "http://localhost:9200")
	viper.SetDefault("blueprint", "resources/concrete_blueprint_doctor.json")
	viper.SetDefault("VDCName", "tubvdc")
	viper.SetDefault("Events", 100)

	viper.SetDefault("wt", 10*time.Second)
	viper.SetDefault("pause", true)

	viper.SetDefault("gen", 1)

	viper.RegisterAlias("elastic", "ElasticSearchURL")

	flag.String("elastic", "http://localhost:9200", "used to define the elasticURL")
	flag.String("blueprint", "resources/concrete_blueprint_doctor.json", "the blueprint to use")
	flag.Int("events", 100, "number of events generated and added to the elasticserach, runs infinitly if value is negative")
	flag.Duration("wt", 10*time.Second, "mean waittime in sec between events")
	flag.Bool("pause", true, "pause betweenEventes")
	flag.String("VDCName", "tubvdc", "VDCName to use")
	flag.Int("gen", 1, "sets the internal generator to use ")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	if viper.GetBool("trace") {
		viper.Set("verbose", true)
	}

	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
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

	gen, err := generator.NewGenerator(mg)

	if err != nil {
		log.Fatalf("failed to start gen %s", err)
	}

	gen.Start()
}
