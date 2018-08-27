package generator

import (
	"math/rand"
	"time"
)

type GMeterValue struct {
	data float64
}

func fromFloat(v float64) *GMeterValue {
	return &GMeterValue{v}
}

func fromInt(v int) *GMeterValue {
	return &GMeterValue{float64(v)}
}

func (g *GMeterValue) AsFloat() float64 {
	return g.data
}

func (g *GMeterValue) AsInt() int32 {
	return int32(g.data)
}

func (g *GMeterValue) AsDuration() time.Duration {
	return time.Duration(int64(g.data * 1000 * 1000 * 1000))
}

func (g *GMeterValue) AsInterface() interface{} {
	return g.data
}

type RandomGenerator struct{}

func NewRandomMeterGenerator() RandomGenerator {
	return RandomGenerator{}
}

func (r RandomGenerator) generate(maximum *float64, minimum *float64, unit string) MeterValue {

	return fromInt(rand.Intn(1000))

}

type ViolationfreeGenerator struct{}

func NewViolationfreeGenerator() ViolationfreeGenerator {
	return ViolationfreeGenerator{}
}

func (r ViolationfreeGenerator) generate(maximum *float64, minimum *float64, unit string) MeterValue {

	var min float64
	if minimum != nil {
		min = *minimum
	} else {
		min = 0
	}
	var max float64
	if maximum != nil {
		max = *maximum
	} else {
		max = 100
	}

	return fromFloat(min + rand.Float64()*(max-min))

}

type TimedViolationGenerator struct {
	triggerTime time.Time
}

func NewTimedViolationGenerator(args []string) TimedViolationGenerator {

	var failureDelay time.Duration
	if len(args) > 0 {
		delay, err := time.ParseDuration(args[0])
		if err != nil {
			log.Error("could not parse delay %ŝ", args[0])
			failureDelay = 60 * time.Second
		} else {
			failureDelay = delay
		}
	} else {
		log.Error("no time set using default")
		failureDelay = 60 * time.Second
	}

	gen := TimedViolationGenerator{triggerTime: time.Now().Add(failureDelay)}

	return gen
}

func (r TimedViolationGenerator) generate(maximum *float64, minimum *float64, unit string) MeterValue {
	var min float64

	var max float64

	if minimum != nil {
		min = *minimum
	} else {
		min = 0
	}

	if maximum != nil {
		max = *maximum
	} else {
		max = 100
	}

	if r.triggerTime.Before(time.Now()) {
		return fromFloat(0)
	}

	return fromFloat(min + rand.Float64()*(max-min))

}
