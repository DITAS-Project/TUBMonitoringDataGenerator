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
	"math"
	"math/rand"
	"time"

	spec "github.com/DITAS-Project/blueprint-go"
)

type GMeterValue struct {
	data      float64
	isNumeric bool
	rawData   *interface{}
}

func fromFloat(v float64) *GMeterValue {
	return &GMeterValue{v, true, nil}
}

func fromInt(v int) *GMeterValue {
	return &GMeterValue{float64(v), false, nil}
}

func fromInterface(value interface{}) *GMeterValue {
	switch i := value.(type) {
	case float64:
		return &GMeterValue{i, true, nil}
	case float32:
		return &GMeterValue{float64(i), true, nil}
	case int64:
		return &GMeterValue{float64(i), true, nil}
	case int32:
		return &GMeterValue{float64(i), true, nil}
	default:
		return &GMeterValue{0, false, &value}
	}
}

func (g *GMeterValue) AsFloat() float64 {
	if g.isNumeric {
		return g.data
	}
	return math.NaN()
}

func (g *GMeterValue) AsInt() int32 {
	if g.isNumeric {
		return int32(g.data)
	}
	return -1
}

func (g *GMeterValue) AsDuration() time.Duration {
	if g.isNumeric {
		return time.Duration(int64(g.data * 1000 * 1000 * 1000))
	}
	return time.Duration(0)
}

func (g *GMeterValue) AsInterface() interface{} {
	if g.isNumeric {
		return g.data
	}
	return *g.rawData
}

type RandomGenerator struct{}

func NewRandomMeterGenerator() RandomGenerator {
	return RandomGenerator{}
}

func (r RandomGenerator) generate(prop spec.MetricPropertyType) MeterValue {

	return fromInt(rand.Intn(1000))

}

type ViolationfreeGenerator struct{}

func NewViolationfreeGenerator() ViolationfreeGenerator {
	return ViolationfreeGenerator{}
}

func (r ViolationfreeGenerator) generate(prop spec.MetricPropertyType) MeterValue {

	if !prop.IsEqualityConstraint() {
		var min float64
		if prop.IsMinimumConstraint() {
			min = *prop.Minimum
		} else {
			min = 0
		}
		var max float64
		if prop.IsMaximumConstraint() {
			max = *prop.Maximum
		} else {
			max = 100
		}

		return fromFloat(min + rand.Float64()*(max-min))
	} else {
		return fromInterface(prop.Value)
	}
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

func (r TimedViolationGenerator) generate(prop spec.MetricPropertyType) MeterValue {

	if r.triggerTime.Before(time.Now()) {
		return fromFloat(0)
	}

	if !prop.IsEqualityConstraint() {
		var min float64
		if prop.IsMinimumConstraint() {
			min = *prop.Minimum
		} else {
			min = 0
		}
		var max float64
		if prop.IsMaximumConstraint() {
			max = *prop.Maximum
		} else {
			max = 100
		}

		return fromFloat(min + rand.Float64()*(max-min))
	} else {
		return fromInterface(prop.Value)
	}

}
