package generator

import (
	"fmt"
	"github.com/DITAS-Project/VDC-Logging-Agent/agent"
	"github.com/DITAS-Project/VDC-Request-Monitor/monitor"
	"github.com/DITAS-Project/VDC-Throughput-Agent/throughputagent"
	"github.com/DITAS-Project/blueprint-go"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
	"math/rand"
	"regexp"
	"time"
)

func (gen *Generator) Generate(methods map[string]blueprint.ExtendedMethods, agentQueue chan agent.ElasticData, trafficQueue chan []throughputagent.TrafficMessage, requestQueue chan monitor.MeterMessage, exchangeQueue chan monitor.ExchangeMessage, mg MeterGenerator) {

	for k, v := range methods {
		//TODO: sampling!
		now := gen.GenerateTrafficData(trafficQueue)
		gen.GenerateAgentData(v, agentQueue, now, k, mg)
		gen.GenerateRequestData(requestQueue, exchangeQueue, k, v)
	}

}

func (gen *Generator) GenerateAgentData(v blueprint.ExtendedMethods, agentQueue chan agent.ElasticData, now time.Time, operationID string, meterGenerator MeterGenerator) {
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

func (gen *Generator) GenerateRequestData(requestQueue chan monitor.MeterMessage, exchangeQueue chan monitor.ExchangeMessage, operationID string, method blueprint.ExtendedMethods) {

	var respTimeProp *blueprint.MetricPropertyType
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
	if viper.GetString("exchange") != "" {
		exchangeQueue <- monitor.ExchangeMessage{
			MeterMessage: msg,
			RequestID:    operationID,
			RequestBody:  RandStringBytesMaskImprSrc(rand.Intn(1024) + 100),
			RequestHeader: map[string][]string{
				"X-DITAS-RequestID":   []string{id},
				"X-DITAS-OperationID": []string{operationID},
			},
		}
	}

	msg = monitor.MeterMessage{
		RequestID:      id,
		OperationID:    operationID,
		RequestTime:    responseTime,
		ResponseLength: rand.Int63n(1024),
		ResponseCode:   (2 + rand.Intn(3)) * 100,
	}
	requestQueue <- msg
	if viper.GetString("exchange") != "" {
		exchangeQueue <- monitor.ExchangeMessage{
			MeterMessage: msg,
			RequestID:    operationID,
			ResponseBody: RandStringBytesMaskImprSrc(rand.Intn(1024) + 100),
			ResponseHeader: map[string][]string{
				"X-DITAS-RequestID":   []string{id},
				"X-DITAS-OperationID": []string{operationID},
			},
		}
	}
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
