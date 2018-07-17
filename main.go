package main

import (
	"flag"

	"github.com/DITAS-Project/TUBMonitoringDataGenerator/generator"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
)

func main() {
	viper.SetDefault("ElasticSearchURL", "http://localhost:9200")
	viper.SetDefault("blueprint", "resources/concrete_blueprint_doctor.json")
	viper.SetDefault("VDCName", "tubvdc")
	viper.SetDefault("Events", 100)

	viper.SetDefault("wt", 10)
	viper.SetDefault("pause", true)

	viper.RegisterAlias("elastic", "ElasticSearchURL")

	flag.String("elastic", "http://localhost:9200", "used to define the elasticURL")
	flag.String("blueprint", "resources/concrete_blueprint_doctor.json", "the blueprint to use")
	flag.Int("events", 100, "number of events generated and added to the elasticserach")
	flag.Int64("wt", 10, "mean waittime in sec between events")
	flag.Bool("pause", true, "pause betweenEventes")
	flag.String("VDCName", "tubvdc", "VDCName to use")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	if viper.GetBool("trace") {
		viper.Set("verbose", true)
	}

	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
	}

	gen, err := generator.NewGenerator()

	if err != nil {
		log.Fatalf("failed to start gen %s", err)
	}

	gen.Start()
}
