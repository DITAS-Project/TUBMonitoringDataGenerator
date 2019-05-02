# TUBMonitoringDataGenerator

This utility generates data based on a given blueprint and saves it in elasticsearch. 
It is intended for testing purposes only.

## Installation
Use `go get` to fetch this repository followed by `go build`. 
This will build an executable of this utility.

## Usage
```
--VDCName string          VDCName to use (default "tubvdc")
--blueprint string        the blueprint to use (default "resources/concrete_blueprint_doctor.json")
--elastic string          used to define the elasticURL (default "http://localhost:9200")
--events int              number of events generated and added to the elasticsearch, runs indefinitely if the value is negative (default 100)
--gen int                 sets the internal generator to use  (default 1)
--pause                   pause between events (default true)
--wt duration             mean wait time in sec between events (default 10s)
```

### Generator
 - 0: Random (will generate Random values for each metric)
 - 1: Violation Free, generates a random value within the bounds of the blueprint
 - 2: Timed Violation, generates valid values within the bounds of the blueprint until a fixed delay than all metrics will be 0. Usage: `./TUBMonitoringDataGenerator --gen 2 20s`
