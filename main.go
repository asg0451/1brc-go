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

	"github.com/cespare/xxhash/v2"
	"github.com/kamstrup/intmap"
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
	station              string
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
// 7.346 s ±  0.144 s - swiss map
// 4.530 s ±  0.077 s - intmap plus remove interning indirection
// 4.134 s ±  0.118 s - guess based split on semi
// 4.418 s ±  0.129 s - use a real hash function to make it more legit. slower :(
//
// graveyard:
// - iterating in reverse order in splitOnSemi
// - using [swiss maps](https://github.com/dolthub/swiss) instead of builtin
// - replacing *stats with stats in maps
// - manual loop var stuff
// - using bytes.IndexByte instead of a for loop to split on lines
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

	resultses := make([]*intmap.Map[uint64, *stats], numWorkers)

	for i := range numWorkers {
		res := intmap.New[uint64, *stats](10_000)
		resultses[i] = res
		chunk := chunks[i]

		wg.Add(1)
		go func() {
			defer wg.Done()

			// would be cool to lock to one cpu using unix.SchedSetaffinity() but it's not available on mac i think :(

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

type worker struct{}

func NewWorker() *worker {
	return &worker{}
}

func (w *worker) run(chunk []byte, res *intmap.Map[uint64, *stats]) error {
	// our chunk is guaranteed to be made of full lines only
	lineStart := 0
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == '\n' {
			// handle line
			stationBs, stationHash, temp, err := w.parseLineBytes(chunk[lineStart:i])
			if err != nil {
				return fmt.Errorf("parsing line %w", err)
			}
			s, ok := res.Get(stationHash)
			if !ok {
				s = &stats{min: temp, max: temp, station: string(stationBs)}
				res.Put(stationHash, s)
			}
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++

			lineStart = i + 1
		}
	}
	return nil
}

func (w *worker) parseLineBytes(line []byte) ([]byte, uint64, float32, error) {
	stationBs, tempStr := w.splitOnSemi(line)

	stationHash := stationHash(stationBs)
	temp := parseFloat(tempStr)
	return stationBs, stationHash, temp, nil
}

func (w *worker) splitOnSemi(bs []byte) ([]byte, []byte) {
	// the format is like ABC;-1.0. the semicolon can only be in a few places from the end: -5 (2 digit pos temp or 1 dig neg), -6 (neg), -4 (1 digit pos temp)
	// the most common variant is 4 digits, then 3, then 5. so check in that order
	if i := len(bs) - 5; bs[i] == ';' {
		return bs[:i], bs[i+1:]
	} else if i := len(bs) - 4; bs[i] == ';' {
		return bs[:i], bs[i+1:]
	} else if i := len(bs) - 6; bs[i] == ';' {
		return bs[:i], bs[i+1:]
	}
	panic("no semicolon found")
}

func stationHash(name []byte) uint64 {
	return xxhash.Sum64(name)
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
func printRes(res *intmap.Map[uint64, *stats]) {
	// {Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
	namesTohashes := getStationsToHashes(res)
	names := maps.Keys(namesTohashes)
	slices.Sort(names)

	fmt.Printf("{")
	for _, name := range names {
		stats, _ := res.Get(namesTohashes[name])
		fmt.Printf("%s=%.1f/%.1f/%.1f,", name, stats.min, stats.sum/stats.count, stats.max)
	}
	fmt.Printf("}\n")
}

func mergeResults(resultses []*intmap.Map[uint64, *stats]) *intmap.Map[uint64, *stats] {
	res := intmap.New[uint64, *stats](resultses[0].Len())
	for _, r := range resultses {
		r.ForEach(func(k uint64, v *stats) {
			s, ok := res.Get(k)
			if !ok {
				s = v
				res.Put(k, s)
			} else {
				s.min = min(s.min, v.min)
				s.max = max(s.max, v.max)
				s.sum += v.sum
				s.count += v.count
			}
		})
	}
	return res
}

func getStationsToHashes(m *intmap.Map[uint64, *stats]) map[string]uint64 {
	names := make(map[string]uint64, m.Len())
	m.ForEach(func(k uint64, s *stats) {
		names[s.station] = k
	})
	return names
}
