package main

import (
	"bufio"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"

	"go.coldcutz.net/go-stuff/utils"
	"golang.org/x/exp/maps"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var traceprofile = flag.String("trace", "", "write trace to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	if *traceprofile != "" {
		f, err := os.Create(*traceprofile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := trace.Start(f); err != nil {
			panic(err)
		}
		defer trace.Stop()
	}

	_, done, log, err := utils.StdSetup()
	if err != nil {
		panic(err)
	}
	done() // use default signal stuff

	if err := run(log); err != nil {
		log.Error("error", "err", err)
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			panic(err)
		}
	}
}

const filename = "measurements_100m.txt"

// input:
// Station name;Temperature

// output:
// min/max/avg per station, alphabetical order
// {Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}

// Input value ranges are as follows:
// Station name: non null UTF-8 string of min length 1 character and max length 100 bytes, containing neither ; nor \n characters. (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)
// Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
// There is a maximum of 10,000 unique station names
// Line endings in the file are \n characters on all platforms
// Implementations must not rely on specifics of a given data set, e.g. any valid station name as per the constraints above and any data distribution (number of measurements per station) must be supported
// The rounding of output values must be done using the semantics of IEEE 754 rounding-direction "roundTowardPositive"

type stats struct {
	min, max, sum, count float64
}

// (for 100m rows)
// 12.338 s ± 0.026 s - start
// 11.989 s ±  0.095 s - increase scanner buffer
// 3.544 s ±  0.061 s - parallelize
// - trace analysis: workers are starved. can we increase the read speed?
// next: custom parsing
func run(_ *slog.Logger) error {
	wg := &sync.WaitGroup{}

	lineses := make(chan []string)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(lineses)

		fh, err := os.Open(filename)
		if err != nil {
			panic("opening file " + err.Error())
		}
		defer fh.Close()

		rdr := bufio.NewScanner(fh)
		rdr.Buffer(make([]byte, 0, 64*1024*1024), 64*1024*1024) // larger buffer

		const lineBufSize = 10 * 1024
		lineBuf := make([]string, 0, lineBufSize)

		for rdr.Scan() {
			line := rdr.Text()
			lineBuf = append(lineBuf, line)
			if len(lineBuf) == cap(lineBuf) {
				lineses <- lineBuf
				lineBuf = make([]string, 0, lineBufSize)
			}
		}
		if err := rdr.Err(); err != nil {
			panic("scanner error " + err.Error())
		}
	}()

	var numWorkers = runtime.NumCPU()

	resultses := make([]map[string]*stats, numWorkers)

	for i := range numWorkers {
		res := make(map[string]*stats) // theoretically this is now ok!
		resultses[i] = res

		wg.Add(1)
		go func() {
			defer wg.Done()

			for lines := range lineses {
				for _, line := range lines {
					station, temp, err := parseLine(line)
					if err != nil {
						panic("parsing line " + err.Error())
					}
					if _, ok := res[station]; !ok {
						res[station] = &stats{min: temp, max: temp}
					}
					s := res[station]
					s.min = min(s.min, temp)
					s.max = max(s.max, temp)
					s.sum += temp
					s.count++
				}
			}
		}()
	}

	wg.Wait()

	res := mergeResults(resultses)

	printRes(res)

	return nil
}

func parseLine(line string) (station string, temp float64, err error) {
	parts := strings.SplitN(line, ";", 2)
	temp, err = strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return "", 0, fmt.Errorf("parsing temperature: %w", err)
	}
	return parts[0], temp, nil
}

func printRes(res map[string]*stats) {
	// {Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
	names := maps.Keys(res)
	slices.Sort(names)

	fmt.Printf("{")
	for _, name := range names {
		stats := res[name]
		fmt.Printf("%s=%.1f/%.1f/%.1f,", name, stats.min, stats.max, stats.sum/stats.count)
	}
	fmt.Printf("}\n")
}

func mergeResults(resultses []map[string]*stats) map[string]*stats {
	res := make(map[string]*stats)
	for _, r := range resultses {
		for k, v := range r {
			if _, ok := res[k]; !ok {
				res[k] = v
			} else {
				res[k].min = min(res[k].min, v.min)
				res[k].max = max(res[k].max, v.max)
				res[k].sum += v.sum
				res[k].count += v.count
			}
		}
	}
	return res
}
