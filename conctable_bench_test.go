package beelog

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
)

// Dear dev, avoid crash on your IDE by running with:
// go test -run none -bench BenchmarkAlgosThroughput -benchtime 1ns -benchmem -v
func BenchmarkConcTableThroughput(b *testing.B) {
	b.SetParallelism(runtime.NumCPU())
	numCommands, diffKeys, writePercent := uint64(1000000), 1000, 50
	log := make(chan pb.Command, numCommands)

	// dummy goroutine that creates a random log of commands
	go createRandomLog(numCommands, diffKeys, writePercent, log)

	chA := make(chan pb.Command, 0)
	chB := make(chan pb.Command, 0)

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	wg.Add(2)

	go runTraditionalLog(chA, numCommands, mu, wg)
	go runConcTable(chB, numCommands, mu, wg)

	// fan-out that output to the different goroutines
	go splitIntoWorkers(log, chA, chB)

	// close the input log channel once all algorithms are executed
	wg.Wait()
	close(log)
}

func createRandomLog(n uint64, dif, wrt int, out chan<- pb.Command) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)

	for i := uint64(0); i < n; i++ {
		cmd := pb.Command{
			Id:  i,
			Key: strconv.Itoa(r.Intn(dif)),
		}

		// WRITE operation
		if cn := r.Intn(100); cn < wrt {
			cmd.Value = strconv.Itoa(r.Int())
			cmd.Op = pb.Command_SET

		} else {
			cmd.Op = pb.Command_GET
		}
		out <- cmd
	}

	// indicates the last command in the log, forcing consumer goroutines to halt
	out <- pb.Command{}
}

func splitIntoWorkers(src <-chan pb.Command, wrks ...chan<- pb.Command) {
	for {
		select {
		case cmd, ok := <-src:
			if !ok {
				return
			}
			for _, ch := range wrks {
				// avoid blocking receive on the sync ch
				go func(dest chan<- pb.Command, c pb.Command) {
					dest <- c
				}(ch, cmd)
			}
		}
	}
}

func runConcTable(log <-chan pb.Command, n uint64, mu *sync.Mutex, wg *sync.WaitGroup) {
	ct := NewConcTable(context.TODO())
	var i uint64
	defer wg.Done()

	start := time.Now()
	for {
		select {
		case cmd, ok := <-log:
			if !ok {
				return
			}

			if i < n {
				ct.Log(cmd)
				i++

			} else {
				// finished logging
				goto BREAK
			}
		}
	}

BREAK:
	// elapsed time to interpret the sequence of commands and construct the tree struct
	construct := time.Since(start)

	var (
		fn, id string
		out    []pb.Command

		// elapsed time to recovery the entire log
		recov time.Duration
	)

	// TODO: call reduce procedure for each configuration

	start = time.Now()
	err := dumpLogIntoFile("./", fn, out)
	if err != nil {
		fmt.Println(err.Error())
	}
	dump := time.Since(start)

	mu.Lock()
	fmt.Println(
		"\n====================",
		"\n===", id,
		"\nRemoved cmds: ", n-uint64(len(out)),
		"\nConstruction Time: ", construct,
		"\nCompactation Time: ", recov,
		"\nInstallation Time:", dump,
		"\n====================",
	)
	mu.Unlock()
}

func runTraditionalLog(log <-chan pb.Command, n uint64, mu *sync.Mutex, wg *sync.WaitGroup) {
	logfile := make([]pb.Command, 0, n)
	var i uint64
	defer wg.Done()

	start := time.Now()
	for {
		select {
		case cmd, ok := <-log:
			if !ok {
				return
			}

			if i < n {
				logfile = append(logfile, cmd)
				i++

			} else {
				// finished logging
				goto BREAK
			}
		}
	}

BREAK:
	construct := time.Since(start)
	fn := "traditionallog-bench.out"

	start = time.Now()
	err := dumpLogIntoFile("./", fn, logfile)
	if err != nil {
		fmt.Println(err.Error())
	}
	dump := time.Since(start)

	mu.Lock()
	fmt.Println(
		"\n====================",
		"\n=== Traditional Log Benchmark",
		"\nRemoved cmds:", n-uint64(len(logfile)),
		"\nConstruction Time:", construct,
		"\nCompactation Time: -",
		"\nInstallation Time:", dump,
		"\n====================",
	)
	mu.Unlock()
}

func dumpLogIntoFile(folder, name string, log []pb.Command) error {
	if _, exists := os.Stat(folder); os.IsNotExist(exists) {
		os.Mkdir(folder, 0744)
	}

	out, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0744)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, cmd := range log {
		_, err = fmt.Fprintf(out, "%d %s %v\n", cmd.Op, cmd.Key, cmd.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
