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

type Generator struct {
	Blueprint spec.BlueprintType
	ESC       *elastic.Client
	ctx       context.Context
}

func NewGenerator() (*Generator, error) {

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

	for i := 0; i < viper.GetInt("Events"); i++ {
		gen.Generate(gen.Blueprint.GetMethodMap(), agentQueue, trafficQueue, requestQueue)
		if viper.GetBool("pause") {
			time.Sleep(time.Duration(rand.Int63n(viper.GetInt64("wt"))) * time.Second)
		}
	}

	stopSignal <- true
	stopSignal <- true
	stopSignal <- true

}

func (gen *Generator) Generate(methods map[string]spec.ExtendedMethods, agentQueue chan agent.ElasticData, trafficQueue chan []throughputagent.TrafficMessage, requestQueue chan monitor.MeterMessage) {

	for k, v := range methods {
		//TODO: sampling!

		id := generateRequestID("127.0.0.1:40123")
		requestQueue <- monitor.MeterMessage{
			Client:        "127.0.0.1:40123",
			OperationID:   k,
			Method:        normilizePath(v.Path),
			Kind:          v.HTTPMethod,
			RequestLenght: rand.Int63n(1024),
			RequestTime:   time.Duration(rand.Int63n(1000000)),
			RequestID:     id,
		}

		requestQueue <- monitor.MeterMessage{
			RequestID:      id,
			OperationID:    k,
			ResponseLength: rand.Int63n(1024),
			ResponseCode:   (2 + rand.Intn(3)) * 100,
		}

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

		agentQueue <- agent.ElasticData{
			Timestamp: now,
			Meter: &agent.MeterMessage{
				OperationID: k,
				Timestamp:   now,
				Value:       rand.Intn(10000),
				Name:        "volume",
				Unit:        "number",
				Raw:         "fake value",
			},
		}

		agentQueue <- agent.ElasticData{
			Timestamp: now,
			Meter: &agent.MeterMessage{
				OperationID: k,
				Timestamp:   now,
				Value:       float64(rand.Intn(100)),
				Name:        "availability",
				Unit:        "percent",
				Raw:         "fake value",
			},
		}
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
