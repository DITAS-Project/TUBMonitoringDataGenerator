package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"regexp"
	"time"

	"github.com/DITAS-Project/TUBUtil/util"
	"github.com/DITAS-Project/VDC-Logging-Agent/agent"
	"github.com/DITAS-Project/VDC-Request-Monitor/monitor"
	"github.com/DITAS-Project/VDC-Throughput-Agent/throughputagent"
	spec "github.com/DITAS-Project/blueprint-go"
	"github.com/olivere/elastic"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logrus.New()

type MeterValue interface {
	AsFloat() float64
	AsInt() int32
	AsDuration() time.Duration
	AsInterface() interface{}
}

type MeterGenerator interface {
	generate(maximum *float64, minimum *float64, unit string) MeterValue
}

type Generator struct {
	Blueprint spec.BlueprintType
	ESC       *elastic.Client
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
	util.WaitForAvailible(ElasticSearchURL, nil)

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
	}, nil
}

func (gen *Generator) Start() {
	stopSignal := make(chan bool)

	agentQueue := make(chan agent.ElasticData)
	trafficQueue := make(chan []throughputagent.TrafficMessage)
	requestQueue := make(chan monitor.MeterMessage)

	go gen.startAgent(agentQueue, stopSignal)

	go gen.startTrafficAgent(trafficQueue, stopSignal)

	go gen.startRequestAgent(requestQueue, stopSignal)

	sendData := func() {
		gen.Generate(gen.Blueprint.GetMethodMap(), agentQueue, trafficQueue, requestQueue, gen.mg)

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

func (gen *Generator) Generate(methods map[string]spec.ExtendedMethods, agentQueue chan agent.ElasticData, trafficQueue chan []throughputagent.TrafficMessage, requestQueue chan monitor.MeterMessage, mg MeterGenerator) {

	for k, v := range methods {
		//TODO: sampling!

		gen.GenerateRequestData(requestQueue, k, v)

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
					Value:       meterGenerator.generate(prop.Maximum, prop.Minimum, prop.Unit).AsInterface(),
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

func (gen *Generator) GenerateRequestData(requestQueue chan monitor.MeterMessage, operationID string, method spec.ExtendedMethods) {

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
		value := gen.mg.generate(respTimeProp.Maximum, respTimeProp.Minimum, respTimeProp.Unit)
		responseTime = value.AsDuration()

	} else {
		responseTime = time.Duration(rand.Int63n(1000000))
	}

	id := generateRequestID("127.0.0.1:40123")
	requestQueue <- monitor.MeterMessage{
		Client:        "127.0.0.1:40123",
		OperationID:   operationID,
		Method:        normilizePath(method.Path),
		Kind:          method.HTTPMethod,
		RequestLenght: rand.Int63n(1024),
		RequestTime:   responseTime,
		RequestID:     id,
	}
	requestQueue <- monitor.MeterMessage{
		RequestID:      id,
		OperationID:    operationID,
		RequestTime:    responseTime,
		ResponseLength: rand.Int63n(1024),
		ResponseCode:   (2 + rand.Intn(3)) * 100,
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

func (gen *Generator) startRequestAgent(queue chan monitor.MeterMessage, QuitChan chan bool) {
	requestAgent, err := monitor.NewElasticReporter(monitor.Configuration{
		ElasticSearchURL: viper.GetString("ElasticSearchURL"),
		VDCName:          viper.GetString("VDCName"),
	}, queue)
	if err != nil {
		log.Fatalf("failed to start RequestMonitor Agent %v", err)
	}

	requestAgent.Start()

	stop := <-QuitChan
	if stop {
		requestAgent.Stop()
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
			insert.Do(gen.ctx)
		case <-QuitChan:
			// We have been asked to stop.
			log.Info("worker%d stopping")
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
