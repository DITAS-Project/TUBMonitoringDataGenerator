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

package generator

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"time"

	util "github.com/DITAS-Project/TUBUtil"
	"github.com/DITAS-Project/VDC-Logging-Agent/agent"
	"github.com/DITAS-Project/VDC-Request-Monitor/monitor"
	"github.com/DITAS-Project/VDC-Throughput-Agent/throughputagent"
	spec "github.com/DITAS-Project/blueprint-go"
	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var logger = logrus.New()
var log = logrus.NewEntry(logger)

func SetLogger(nLogger *logrus.Logger) {
	logger = nLogger
}

func SetLog(entty *logrus.Entry) {
	log = entty
}

type MeterValue interface {
	AsFloat() float64
	AsInt() int32
	AsDuration() time.Duration
	AsInterface() interface{}
}

type MeterGenerator interface {
	generate(prop spec.MetricPropertyType) MeterValue
}

type Generator struct {
	Blueprint spec.BlueprintType
	ESC       *elastic.Client
	exchange  string
	mg        MeterGenerator
	ctx       context.Context
}

func NewGenerator(mg MeterGenerator) (*Generator, error) {

	path := viper.GetString("blueprint")
	var blueprint spec.BlueprintType

	raw, err := ioutil.ReadFile(path)
	if err != nil {
		log.Errorf("Error reading blueprint from %s: %s", path, err.Error())
		return nil, err
	} else {
		err = json.Unmarshal(raw, &blueprint)
		if err != nil {
			log.Errorf("Error reading blueprint: %s", err.Error())
			return nil, err
		}
	}

	ElasticSearchURL := viper.GetString("ElasticSearchURL")

	//set logger
	monitor.SetLog(log)
	monitor.SetLogger(logger)
	agent.SetLog(log)
	agent.SetLogger(logger)
	util.SetLogger(logger)
	util.SetLog(log)

	log.Debug("Waiting for ElasticSerach")
	util.WaitForAvailible(ElasticSearchURL, nil)

	if viper.GetString("exchange") != "" {
		log.Infof("exchange consumer is configured, waiting until ready")
		util.WaitForAvailible(viper.GetString("exchange"), nil)
	}

	log.Infof("using %s for elastic", ElasticSearchURL)

	client, err := elastic.NewSimpleClient(
		elastic.SetURL(ElasticSearchURL),
		elastic.SetErrorLog(log),
		elastic.SetInfoLog(log),
	)

	if err != nil {
		log.Errorf("unable to create elastic client tracer: %+v\n", err)
		return nil, err
	}

	return &Generator{
		Blueprint: blueprint,
		ESC:       client,
		ctx:       context.Background(),
		mg:        mg,
		exchange:  viper.GetString("exchange"),
	}, nil
}

func (gen *Generator) Start() {
	stopSignal := make(chan bool)

	log.Infof("config: pause:%t events:%d wt:%d", viper.GetBool("pause"), viper.GetInt("Events"), viper.GetDuration("wt"))

	agentQueue := make(chan agent.ElasticData)
	trafficQueue := make(chan []throughputagent.TrafficMessage)
	requestQueue := make(chan monitor.MeterMessage)
	exchangeQueue := make(chan monitor.ExchangeMessage)

	go gen.startAgent(agentQueue, stopSignal)

	go gen.startTrafficAgent(trafficQueue, stopSignal)

	go gen.startRequestAgent(requestQueue, exchangeQueue, stopSignal)

	sendData := func() {
		gen.Generate(gen.Blueprint.GetMethodMap(), agentQueue, trafficQueue, requestQueue, exchangeQueue, gen.mg)

		if viper.GetBool("pause") {
			time.Sleep(viper.GetDuration("wt"))
		}
	}

	if viper.GetInt("Events") > 0 {
		log.Debug("sending limited number of events")
		for i := 0; i < viper.GetInt("Events"); i++ {
			sendData()
			log.Debugf("send %d event %d remaining",i , viper.GetInt("Events")-i)
		}
	} else {
		log.Debug("sending unlimited number of events")
		for {
			sendData()
		}
	}

	stopSignal <- true
	stopSignal <- true
	stopSignal <- true

}


func (gen *Generator) startRequestAgent(queue chan monitor.MeterMessage, exchange chan monitor.ExchangeMessage, QuitChan chan bool) {

	requestAgent, err := monitor.NewElasticReporter(monitor.Configuration{
		ElasticSearchURL: viper.GetString("ElasticSearchURL"),
		VDCName:          viper.GetString("VDCName"),
	}, queue)
	if err != nil {
		log.Fatalf("failed to start ElasticReporter Agent %v", err)
	}
	var exchangeAgent monitor.ExchangeReporter
	if viper.GetString("exchange") != "" {
		exchangeAgent, err = monitor.NewExchangeReporter(viper.GetString("exchange"), exchange)

		if err != nil {
			log.Fatalf("failed to start ExchangeReporter Agent %v", err)
		}
	}

	requestAgent.Start()
	if viper.GetString("exchange") != "" {
		exchangeAgent.Start()
	}

	stop := <-QuitChan
	if stop {
		requestAgent.Stop()
		if viper.GetString("exchange") != "" {
			exchangeAgent.Stop()
		}
	}

}

func (gen *Generator) startTrafficAgent(queue chan []throughputagent.TrafficMessage, QuitChan chan bool) {
	for {

		select {
		case work := <-queue:
			insert := throughputagent.CreateBulkInsert(gen.ESC, viper.GetString("VDCName"))
			for _, msg := range work {
				throughputagent.InsertIntoElastic(msg, insert)
			}
			_, err := insert.Do(gen.ctx)
			if err != nil {
				log.Debugf("failed to persist traffic data %+v", err)
			}
		case <-QuitChan:
			// We have been asked to stop.
			log.Info("worker stopping")
			return
		}
	}
}

func (gen *Generator) startAgent(queue chan agent.ElasticData, QuitChan chan bool) {
	viper.SetDefault("tracing", false)
	agt, err := agent.CreateAgent(agent.Configuration{
		VDCName:          viper.GetString("VDCName"),
		ElasticSearchURL: viper.GetString("ElasticSearchURL"),
	})

	if err != nil {
		log.Fatalf("could not start agent %+v", err)
	}

	for {

		select {
		case work := <-queue:
			err := agt.AddToES(work)
			if err != nil{
				log.Debugf("Failed to send agent data %+v",err)
			}
		case <-QuitChan:
			// We have been asked to stop.
			log.Info("worker%d stopping")
			return
		}
	}
}
