package generator

import (
	"math/rand"
	"time"
)

type RandomGenerator struct{}

func NewRandomMeterGenerator() RandomGenerator {
	return RandomGenerator{}
}

func (r RandomGenerator) generate(maximum *float64, minimum *float64, unit string) interface{} {

	return rand.Intn(1000)

}

type ViolationfreeGenerator struct{}

func NewViolationfreeGenerator() ViolationfreeGenerator {
	return ViolationfreeGenerator{}
}

func (r ViolationfreeGenerator) generate(maximum *float64, minimum *float64, unit string) interface{} {

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

	return (min + rand.Float64()*(max-min))

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
		log.Error("no time set usig defaul")
		failureDelay = 60 * time.Second
	}

	gen := TimedViolationGenerator{triggerTime: time.Now().Add(failureDelay)}

	return gen
}

func (r TimedViolationGenerator) generate(maximum *float64, minimum *float64, unit string) interface{} {
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
		return 0
	}

	return (min + rand.Float64()*(max-min))

}
