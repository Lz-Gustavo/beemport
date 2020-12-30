package beelog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/golang/protobuf/proto"
)

func TestConcTableRecovEntireLog(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	concRecov := false

	cfgs := []LogConfig{
		{
			Inmem:   false,
			KeepAll: true,
			Tick:    Interval,
			Period:  100,
			Fname:   "./logstate.log",
		},
	}

	for _, cf := range cfgs {
		var st Structure
		var err error

		// clean state before creating
		if err := cleanAllLogStates(); err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		st, err = generateRandConcTable(nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		if concRecov {
			ch, num, err := st.(*ConcTable).RecovEntireLogConc()
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}
			fmt.Println("now reading ch...")

			for {
				raw, ok := <-ch
				if !ok { // closed ch
					fmt.Println("closed!")
					break
				}
				fmt.Println("got one, deserializing")

				log, err := deserializeRawLogStream(raw, num)
				if err == io.EOF {
					t.Log("empty log")
					t.Fail()

				} else if err != nil {
					t.Log("error while deserializing log, err:", err.Error())
					t.FailNow()
				}

				t.Log("got one log:", log)
				// TODO: test log...
			}

		} else {
			raw, num, err := st.(*ConcTable).RecovEntireLog()
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			log, err := deserializeRawLogStream(raw, num)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			t.Log("got log:", log)
			// TODO: test log...
		}
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableLatencyMeasurementAndSync(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	cfgs := []LogConfig{
		{
			Inmem:   false,
			KeepAll: true,
			Sync:    true,
			Measure: true,
			Tick:    Interval,
			Period:  200,
			Fname:   "./logstate.log",
		},
	}

	for _, cf := range cfgs {
		// clean state before creating
		err := cleanAllLogStates()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// latency is already recorded while being generated
		ct, err := generateRandConcTable(nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// persist latency metrics by during shutdown
		ct.Shutdown()
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableParallelIO(t *testing.T) {
	nCmds, wrt, dif := uint64(800), 50, 200

	primDir := t.TempDir()
	secdDir := t.TempDir()
	cfgs := []LogConfig{
		{
			KeepAll:     true,
			Tick:        Interval,
			Period:      200,
			Fname:       primDir + "/logstate.log",
			ParallelIO:  true,
			SecondFname: secdDir + "/logstate2.log",
		},
	}

	for _, cf := range cfgs {
		// log files should be interchanged between primary and second fns
		_, err := generateRandConcTable(nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// must wait concurrent persistence...
		time.Sleep(time.Second)

		logsPrim, err := filepath.Glob(primDir + "/*.log")
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		logsSecd, err := filepath.Glob(secdDir + "/*.log")
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		if p, s := len(logsPrim), len(logsSecd); p != s {
			t.Log("With an even interval config, expected primary and secondary locations to have the same number of log files")
			t.Log("PRIMARY HAS:", p)
			t.Log("SECONDARY HAS:", s)
			t.FailNow()
		}
	}
}

// deserializeRawLogStream emulates the same procedure implemented by a recov
// replica, interpreting the serialized log stream received from RecovEntireLog
// different calls.
func deserializeRawLogStream(stream []byte, size int) ([]pb.Command, error) {
	rd := bytes.NewReader(stream)
	cmds := make([]pb.Command, 0, 256*size)

	for i := 0; i < size; i++ {
		// read the retrieved log interval
		var f, l uint64
		var ln int
		_, err := fmt.Fscanf(rd, "%d\n%d\n%d\n", &f, &l, &ln)
		if err != nil {
			return nil, err
		}

		for j := 0; j < ln; j++ {
			var commandLength int32
			err = binary.Read(rd, binary.BigEndian, &commandLength)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			serializedCmd := make([]byte, commandLength)
			_, err = rd.Read(serializedCmd)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}

			c := &pb.Command{}
			err = proto.Unmarshal(serializedCmd, c)
			if err != nil {
				fmt.Println("could not parse")
				return nil, err
			}
			cmds = append(cmds, *c)
		}

		var eol string
		_, err = fmt.Fscanf(rd, "\n%s\n", &eol)
		if err != nil {
			return nil, err
		}

		if eol != "EOL" {
			return nil, fmt.Errorf("expected EOL flag, got '%s'", eol)
		}
	}
	return cmds, nil
}

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
