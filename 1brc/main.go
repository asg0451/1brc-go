package main

import "go.coldcutz.net/go-stuff/utils"

func main() {
	_, done, log, err := utils.StdSetup()
	if err != nil {
		panic(err)
	}
	done() // use default signal stuff

	if err := run(); err != nil {
		log.Error("error", "err", err)
	}
}

const filename = "weather_stations.csv"

func run() error {
	return nil
}
