package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"sync"
	"syscall"

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
		os.Exit(1)
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

const filename = "measurements.txt"

type stats struct {
	min, max, sum, count float32
}

// invocation: $ ./make.sh && GOGC=off hyperfine -w1 -m5 ./bin/1brc

// (for 100m rows)
// 12.338 s ± 0.026 s - start
// 11.989 s ±  0.095 s - increase scanner buffer
// 3.544 s ±  0.061 s - parallelize
// - trace analysis: workers are starved. can we increase the read speed?
// 3.665 s ±  0.133 s - mmap char by char (a bit slower)
// 3.329 s ±  0.075 s - mmap with buffered reading
// 2.490 s ±  0.087 s - same but with GOGC=off. now we're just cpu bound i think
// 2.402 s ±  0.068 s - custom float parsing
// 1.538 s ±  0.068 s - custom semicolon splitting
// 1.239 s ±  0.048 s - prealllocating hash tables with 10k size
// 1.116 s ±  0.056 s - interning station names
// ** swapping to 1b rows **
// 11.552 s ±  0.409 s - above
// 11.542 s ±  0.083 s - switch to float32s
// 11.276 s ±  0.385 s - optimized parsefloat more
// 10.921 s ±  0.275 s - fixed unnecessary conversions in hash+interning
// 10.334 s ±  0.364 s - pgo
// 9.893 s ±  0.162 s  - manual mmap
// 9.389 s ±  0.105 s  - manual line handling
// 9.085 s ±  0.121 s  - cleanup + better run?
// 7.818 s ±  0.250 s - switch from hash to just byte sum
//
// graveyard:
// - iterating in reverse order in splitOnSemi
// - using [swiss maps](https://github.com/dolthub/swiss) instead of builtin
// - replacing *stats with stats in maps
// - manual loop var stuff
func run(log *slog.Logger) error {
	numWorkers := runtime.NumCPU()

	wg := &sync.WaitGroup{}

	mmappedFile, close, err := setupMmap()
	if err != nil {
		return fmt.Errorf("setting up mmap %w", err)
	}
	defer close()

	fileLen := len(mmappedFile)

	type job struct {
		start, end int // inclusive start, exclusive end
	}

	// divvy up the file. each worker gets a slice of the file but we need to make sure we don't split in the middle of a line
	chunks := make([]job, numWorkers)
	chunkSize := fileLen / numWorkers
	nextStart := 0
	for ci := range chunks {
		start := nextStart
		chunks[ci].start = start
		// if this is the last chunk, just take the rest of the file
		if ci == numWorkers-1 {
			chunks[ci].end = fileLen
			break
		}
		// find the last EOL before the end of the chunk
		theoreticalEnd := start + chunkSize
		for i := theoreticalEnd; i > start; i-- {
			if mmappedFile[i] == '\n' {
				chunks[ci].end = i
				break
			}
		}
		nextStart = chunks[ci].end + 1
	}

	resultses := make([]map[string]*stats, numWorkers)

	for i := range numWorkers {
		res := make(map[string]*stats, 10_000) // theoretically this is now ok!
		resultses[i] = res
		chunk := chunks[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			w := NewWorker()
			if err := w.run(mmappedFile[chunk.start:chunk.end], res); err != nil {
				log.Error("worker error", "err", err)
			}
		}()
	}

	wg.Wait()

	res := mergeResults(resultses)

	printRes(res)

	return nil
}

func setupMmap() ([]byte, func(), error) {
	// custom mmap since exp/mmap's ReaderAt does copies
	f, err := os.Open(filename)
	if err != nil {
		return nil, func() {}, fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, func() {}, fmt.Errorf("statting file: %w", err)
	}

	size := fi.Size()

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, func() {}, fmt.Errorf("mmap: %w", err)
	}

	return data, func() { _ = syscall.Munmap(data) }, nil
}

type worker struct {
	stationNameHashes map[uint32]string
}

func NewWorker() *worker {
	return &worker{
		stationNameHashes: make(map[uint32]string, 10_000),
	}
}

func (w *worker) run(chunk []byte, res map[string]*stats) error {
	// our chunk is guaranteed to be made of full lines only
	lineStart := 0
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == '\n' {
			// handle line
			station, temp, err := w.parseLineBytes(chunk[lineStart:i])
			if err != nil {
				return fmt.Errorf("parsing line %w", err)
			}
			if _, ok := res[station]; !ok {
				res[station] = &stats{min: temp, max: temp}
			}
			s := res[station]
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++

			lineStart = i + 1
		}
	}
	return nil
}

func (w *worker) parseLineBytes(line []byte) (string, float32, error) {
	stationBs, tempStr := w.splitOnSemi(line)

	// use or create interned station name
	hash := stationHash(stationBs)
	station, ok := w.stationNameHashes[hash]
	if !ok {
		station = string(stationBs)
		w.stationNameHashes[hash] = station
	}

	temp := parseFloat(tempStr)
	return station, temp, nil
}

func (w *worker) splitOnSemi(bs []byte) ([]byte, []byte) {
	// you might think it would be faster to go from the back but turns out no
	for i, b := range bs {
		if b == ';' {
			return bs[:i], bs[i+1:]
		}
	}
	panic("no semicolon found")
}

func stationHash(name []byte) uint32 {
	hash := uint32(0)
	for _, b := range name {
		hash += uint32(b)
	}
	return hash
}

func parseFloat(bs []byte) float32 {
	// Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
	sign := float32(1.)
	if bs[0] == '-' {
		sign = -1.
		bs = bs[1:]
	}

	intPart := bs[:len(bs)-2]
	fracPart := bs[len(bs)-1] - '0'

	var ip int
	if len(intPart) == 2 {
		ip = int((intPart[0]-'0')*10 + (intPart[1] - '0'))
	} else {
		ip = int(intPart[0] - '0')
	}

	return sign * (float32(ip) + float32(fracPart)/10)
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
