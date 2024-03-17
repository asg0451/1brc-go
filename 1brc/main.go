package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"sync"

	"go.coldcutz.net/go-stuff/utils"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/mmap"
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

// invocation: $ go build -o bin/ ./1brc && GOGC=off hyperfine -w1 ./bin/1brc

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
//
// graveyard:
// - iterating in reverse order in splitOnSemi
// - using [swiss maps](https://github.com/dolthub/swiss) instead of builtin
// - replacing *stats with stats in maps
// - manual loop var stuff
func run(log *slog.Logger) error {
	numWorkers := runtime.NumCPU()

	wg := &sync.WaitGroup{}

	rdr, err := mmap.Open(filename)
	if err != nil {
		return fmt.Errorf("mmapping file %w", err)
	}
	defer rdr.Close()
	fileLen := rdr.Len()

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
			if rdr.At(i) == '\n' {
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
			if err := w.run(chunk, rdr, res); err != nil {
				log.Error("worker error", "err", err)
			}
		}()
	}

	wg.Wait()

	res := mergeResults(resultses)

	printRes(res)

	return nil
}

type job struct {
	start, end int // inclusive start, exclusive end
}

type worker struct {
	hasher            hash.Hash32
	stationNameHashes map[int64]string
}

func NewWorker() *worker {
	return &worker{
		hasher:            fnv.New32(),
		stationNameHashes: make(map[int64]string, 10_000),
	}
}

func (w *worker) run(chunk job, rdr io.ReaderAt, res map[string]*stats) error {
	srdr := io.NewSectionReader(rdr, int64(chunk.start), int64(chunk.end-chunk.start))
	scanner := bufio.NewScanner(srdr)
	sb := make([]byte, 0, 1024*1024)
	scanner.Buffer(sb, cap(sb))
	for scanner.Scan() {
		line := scanner.Bytes()

		station, temp, err := w.parseLineBytes(line)
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
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("parsing line %w", err)
	}

	return nil
}

func (w *worker) parseLineBytes(line []byte) (string, float32, error) {
	stationBs, tempStr := w.splitOnSemi(line)

	// use or create interned station name
	// this is a bit sus because we could get hash collisions. odds: 10k / 2^32 = 2.3e-6
	w.hasher.Reset()
	_, _ = w.hasher.Write(stationBs)
	hash := w.hasher.Sum32()
	station, ok := w.stationNameHashes[int64(hash)]
	if !ok {
		station = string(stationBs)
		w.stationNameHashes[int64(hash)] = station
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

func parseFloat(bs []byte) float32 {
	// Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
	sign := float32(1.)
	if bs[0] == '-' {
		sign = -1.
		bs = bs[1:]
	}
	intPart := bs
	for i, b := range bs {
		if b == '.' {
			intPart = bs[:i]
			continue
		}
		bs[i] -= '0'
	}

	// parse the int part
	ip := 0
	for i := 0; i < len(intPart); i++ {
		ip *= 10
		ip += int(intPart[i])
	}

	// parse the fractional part
	fp := int(bs[len(bs)-1])

	return sign * (float32(ip) + float32(fp)/10)
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
