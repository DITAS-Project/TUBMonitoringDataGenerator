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
	"fmt"
	"io/ioutil"
	"math/rand"
	"regexp"
	"time"

	"github.com/DITAS-Project/TUBUtil"
	"github.com/DITAS-Project/VDC-Logging-Agent/agent"
	"github.com/DITAS-Project/VDC-Request-Monitor/monitor"
	"github.com/DITAS-Project/VDC-Throughput-Agent/throughputagent"
	spec "github.com/DITAS-Project/blueprint-go"
	"github.com/olivere/elastic"
	uuid "github.com/satori/go.uuid"
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

	util.SetLogger(logger)
	util.SetLog(log)

	util.WaitForAvailible(ElasticSearchURL, nil)

	if viper.GetString("exchange") != ""{
		log.Infof("exchange consumer is configured, waiting until ready")
		util.WaitForAvailible(viper.GetString("exchange") , nil)
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
		exchange: viper.GetString("exchange") ,
	}, nil
}

func (gen *Generator) Start() {
	stopSignal := make(chan bool)

	agentQueue := make(chan agent.ElasticData)
	trafficQueue := make(chan []throughputagent.TrafficMessage)
	requestQueue := make(chan monitor.MeterMessage)
	exchangeQueue := make(chan monitor.ExchangeMessage)

	go gen.startAgent(agentQueue, stopSignal)

	go gen.startTrafficAgent(trafficQueue, stopSignal)

	go gen.startRequestAgent(requestQueue,exchangeQueue, stopSignal)

	sendData := func() {
		gen.Generate(gen.Blueprint.GetMethodMap(), agentQueue, trafficQueue, requestQueue,exchangeQueue, gen.mg)

		if viper.GetBool("pause") {
			time.Sleep(viper.GetDuration("wt"))
		}
	}

	if viper.GetInt("Events") > 0 {
		for i := 0; i < viper.GetInt("Events"); i++ {
			sendData()
		}
	} else {
		for {
			sendData()
		}
	}

	stopSignal <- true
	stopSignal <- true
	stopSignal <- true

}

func (gen *Generator) Generate(methods map[string]spec.ExtendedMethods, agentQueue chan agent.ElasticData, trafficQueue chan []throughputagent.TrafficMessage, requestQueue chan monitor.MeterMessage, exchangeQueue chan monitor.ExchangeMessage, mg MeterGenerator) {

	for k, v := range methods {
		//TODO: sampling!

		gen.GenerateRequestData(requestQueue,exchangeQueue, k, v)

		now := gen.GenerateTrafficData(trafficQueue)

		gen.GenerateAgentData(v, agentQueue, now, k, mg)
	}

}

func (gen *Generator) GenerateAgentData(v spec.ExtendedMethods, agentQueue chan agent.ElasticData, now time.Time, operationID string, meterGenerator MeterGenerator) {
	for _, du := range v.Method.Attributes.DataUtility {
		for name, prop := range du.Properties {

			if name == "responseTime" {
				continue
			}

			agentQueue <- agent.ElasticData{
				Timestamp: now,
				Meter: &agent.MeterMessage{
					OperationID: operationID,
					Timestamp:   now,
					Value:       meterGenerator.generate(prop).AsInterface(),
					Name:        name,
					Unit:        prop.Unit,
					Raw:         "fake value",
				},
			}
		}
	}
}

func (gen *Generator) GenerateTrafficData(trafficQueue chan []throughputagent.TrafficMessage) time.Time {
	traffic := make([]throughputagent.TrafficMessage, rand.Intn(10))
	now := time.Now()
	for i := 0; i < len(traffic); i++ {
		rx := rand.Intn(4096)
		tx := rand.Intn(4096)
		traffic[i] = throughputagent.TrafficMessage{
			Timestamp: now,
			Component: fmt.Sprintf("127.0.0.1:%d", (1000 * rand.Intn(36))),
			Send:      rx,
			Recived:   tx,
			Total:     rx + tx,
		}
	}
	trafficQueue <- traffic
	return now
}

///////////////////////////
// FROM https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
    letterIdxBits = 6                    // 6 bits to represent a letter index
    letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
    letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)


var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) string {
    b := make([]byte, n)
    // A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
    for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
        if remain == 0 {
            cache, remain = src.Int63(), letterIdxMax
        }
        if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
            b[i] = letterBytes[idx]
            i--
        }
        cache >>= letterIdxBits
        remain--
    }

    return string(b)
}
///////////////////////////////////

func (gen *Generator) GenerateRequestData(requestQueue chan monitor.MeterMessage,exchangeQueue chan monitor.ExchangeMessage, operationID string, method spec.ExtendedMethods) {

	var respTimeProp *spec.MetricPropertyType
	for _, du := range method.Method.Attributes.DataUtility {
		for name, prop := range du.Properties {
			if name == "responseTime" {
				respTimeProp = &prop
				break
			}
		}
	}

	var responseTime time.Duration
	if respTimeProp != nil {
		value := gen.mg.generate(*respTimeProp)
		responseTime = value.AsDuration()

	} else {
		responseTime = time.Duration(rand.Int63n(1000000))
	}

	id := generateRequestID("127.0.0.1:40123")
	msg := monitor.MeterMessage{
		Client:        "127.0.0.1:40123",
		OperationID:   operationID,
		Method:        normilizePath(method.Path),
		Kind:          method.HTTPMethod,
		RequestLenght: rand.Int63n(1024),
		RequestTime:   responseTime,
		RequestID:     id,
	}

	requestQueue <- msg
	exchangeQueue <- monitor.ExchangeMessage{
		MeterMessage:msg,
		RequestID: operationID,
		RequestBody:RandStringBytesMaskImprSrc(rand.Intn(1024)+100),
		RequestHeader:map[string][]string{
			"X-DITAS-RequestID":[]string{id},
			"X-DITAS-OperationID":[]string{operationID},
		},
	}

	msg = monitor.MeterMessage{
		RequestID:      id,
		OperationID:    operationID,
		RequestTime:    responseTime,
		ResponseLength: rand.Int63n(1024),
		ResponseCode:   (2 + rand.Intn(3)) * 100,
	}
	requestQueue <- msg
	exchangeQueue <- monitor.ExchangeMessage{
		MeterMessage:msg,
		RequestID: operationID,
		ResponseBody:RandStringBytesMaskImprSrc(rand.Intn(1024)+100),
		ResponseHeader:map[string][]string{
			"X-DITAS-RequestID":[]string{id},
			"X-DITAS-OperationID":[]string{operationID},
		},
	}
}

var r *regexp.Regexp = regexp.MustCompile("{[a-zA-Z0-9\\-_]*}")

func normilizePath(in string) string {
	return r.ReplaceAllStringFunc(in, func(s string) string {
		return fmt.Sprintf("%d", rand.Intn(1000))
	})
}

func generateRequestID(remoteAddr string) string {
	now := time.Now()
	return uuid.NewV5(uuid.NamespaceX500, fmt.Sprintf("%s-%d-%d", remoteAddr, now.Day(), now.Minute())).String()
}

func (gen *Generator) startRequestAgent(queue chan monitor.MeterMessage,exchange chan monitor.ExchangeMessage, QuitChan chan bool) {

	monitor.SetLogger(logger)
	monitor.SetLog(log)

	requestAgent, err := monitor.NewElasticReporter(monitor.Configuration{
		ElasticSearchURL: viper.GetString("ElasticSearchURL"),
		VDCName:          viper.GetString("VDCName"),
	}, queue)
	if err != nil {
		log.Fatalf("failed to start ElasticReporter Agent %v", err)
	}
	var exchangeAgent monitor.ExchangeReporter
	if viper.GetString("exchange") != ""{
		exchangeAgent,err = monitor.NewExchangeReporter(viper.GetString("exchange"),exchange)

		if err != nil {
			log.Fatalf("failed to start ExchangeReporter Agent %v", err)
		}
	}
	

	requestAgent.Start()
	if viper.GetString("exchange") != ""{
		exchangeAgent.Start()
	}

	stop := <-QuitChan
	if stop {
		requestAgent.Stop()
		if viper.GetString("exchange") != ""{
			exchangeAgent.Stop()
		}
	}

}

func (gen *Generator) startTrafficAgent(queue chan []throughputagent.TrafficMessage, QuitChan chan bool) {
	throughputagent.SetLogger(logger)
	throughputagent.SetLog(log)
	for {

		select {
		case work := <-queue:
			insert := throughputagent.CreateBulkInsert(gen.ESC, viper.GetString("VDCName"))
			for _, msg := range work {
				throughputagent.InsertIntoElastic(msg, insert)
			}
			insert.Do(gen.ctx)
		case <-QuitChan:
			// We have been asked to stop.
			log.Info("worker%d stopping")
			return
		}
	}
}

func (gen *Generator) startAgent(queue chan agent.ElasticData, QuitChan chan bool) {
	agent.SetLogger(logger)
	agent.SetLog(log)

	viper.SetDefault("tracing", false)
	agt, err := agent.CreateAgent(agent.Configuration{
		VDCName:          viper.GetString("VDCName"),
		ElasticSearchURL: viper.GetString("ElasticSearchURL"),
	})

	if err != nil {
		log.Fatalf("could not start agent %+v", err)
	}

	//agent data
	agt.InitES()

	for {

		select {
		case work := <-queue:
			agt.AddToES(work)
		case <-QuitChan:
			// We have been asked to stop.
			log.Info("worker%d stopping")
			return
		}
	}
}
