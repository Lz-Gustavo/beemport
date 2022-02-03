package beemport

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beemport/pb"
	"google.golang.org/protobuf/proto"
)

const (
	defaultSizeOnCommandBytes = 32
)

func TestConcTableLog(t *testing.T) {
	first := uint64(1)
	n := uint64(1000)
	ct := NewConcTable(context.TODO())

	// populate some SET commands
	for i := first; i < n; i++ {
		cmd := &pb.Entry{
			Id:      i,
			WriteOp: true,
			Key:     int64(i),
			Command: generateRandByteSlice(),
		}

		if err := ct.Log(cmd); err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
	}
	l := ct.Len()
	if l != n-first {
		t.Log(l, "commands, expected", n-first)
		t.FailNow()
	}

	// log another GET
	if err := ct.Log(&pb.Entry{Id: n}); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	// GET command shouldnt increase size, but modify 'avl.last' index
	if l != ct.Len() {
		t.Log(l, "commands, expected", ct.Len())
		t.FailNow()
	}

	if ct.logs[ct.cursor].first != first {
		t.Log("first cmd index is", ct.logs[ct.cursor].first, ", expected", first)
		t.FailNow()
	}
	if ct.logs[ct.cursor].last != n {
		t.Log("last cmd index is", ct.logs[ct.cursor].last, ", expected", n)
		t.FailNow()
	}
}

func TestConcTableDifferentRecoveries(t *testing.T) {
	// Requesting the last matching index (i.e. n == nCmds) is mandatory on Immediately
	// and Interval configurations.
	nCmds, wrt, dif := uint64(2000), 50, 10
	p, n := uint64(10), uint64(2000)

	// Interval config is intentionally delayed (i.e. the number of cmds is always less
	// than reduce period) to allow a state comparison between the different routines.
	// The same is required on ConcTable's Immediately config, the number of tested cmds
	// is always less than 'resetOnImmediately' const.
	//
	// TODO: Later implement an exclusive test procedure for these scenarios.
	cfgs := []LogConfig{
		{ // immediately inmem
			Tick:  Immediately,
			Inmem: true,
		},
		{ // delayed inmem
			Tick:  Delayed,
			Inmem: true,
		},
		{ // interval inmem
			Tick:   Interval,
			Period: 10000,
			Inmem:  true,
		},
		{ // immediately disk
			Tick:  Immediately,
			Inmem: false,
			Fname: "./logstate.log",
		},
		{ // delayed disk
			Tick:  Delayed,
			Inmem: false,
			Fname: "./logstate.log",
		},
		{ // interval disk
			Tick:   Interval,
			Period: 10000,
			Inmem:  false,
			Fname:  "./logstate.log",
		},
	}

	for i, cf := range cfgs {
		// delete current logstate, if any, in order to avoid conflict on
		// persistent interval scenarios
		if cf.Tick == Interval && !cf.Inmem {
			if err := cleanAllLogStates(); err != nil {
				t.Log(err.Error())
				t.FailNow()
			}
		}

		var ct *ConcTable
		var err error

		// Currently logs must be re-generated for each different config, which
		// prohibits the generation of an unique log file for all configs. An
		// unique log is not needed on a unit test scenario, since the idea is
		// not to compare these strategies, only tests its correctness.
		ct, err = generateRandConcTable(nCmds, wrt, dif, cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Log("===Executing ConcTable Test Case #", i)

		// the compacted log used for later comparison
		view := ct.retrieveCurrentViewCopy()
		redLog := generateLogFromTable(&view)

		// wait for new state attribution on concurrent structures.
		time.Sleep(time.Second)

		log, err := ct.Recov(p, n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		if !logsAreEquivalent(redLog, log) {
			if logsAreOnlyDelayed(redLog, log) {
				t.Log("Logs are not equivalent, but only delayed")
			} else {
				t.Log("Logs are completely incoherent")
				t.Log("REDC:", redLog)
				t.Log("RECV:", log)
				t.FailNow()
			}
		}

		if len(redLog) == 0 {
			t.Log("Both logs are empty")
			t.Log("REDC:", redLog)
			t.Log("RECV:", log)
			t.FailNow()
		}
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableRecovBytesInterpretation(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	p, n := uint64(100), uint64(1500)

	cfgs := []LogConfig{
		{ // inmem byte recov
			Tick:  Delayed,
			Inmem: true,
		},
		{ // disk byte recov
			Tick:  Delayed,
			Inmem: false,
			Fname: "./logstate.log",
		},
	}

	for i, cf := range cfgs {
		var ct *ConcTable
		var err error

		ct, err = generateRandConcTable(nCmds, wrt, dif, cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Log("===Executing ConcTable Test Case #", i)

		// the compacted log used for later comparison
		view := ct.retrieveCurrentViewCopy()
		redLog := generateLogFromTable(&view)

		// wait for new state attribution on concurrent structures...
		time.Sleep(time.Second)

		raw, err := ct.RecovBytes(p, n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		log, err := deserializeRawLog(raw)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		if !logsAreEquivalent(redLog, log) {
			if logsAreOnlyDelayed(redLog, log) {
				t.Log("Logs are not equivalent, but only delayed")
			} else {
				t.Log("Logs are completely incoherent")
				t.Log("REDC:", redLog)
				t.Log("RECV:", log)
				t.FailNow()
			}
		}

		if len(redLog) == 0 {
			t.Log("Both logs are empty")
			t.Log("REDC:", redLog)
			t.Log("RECV:", log)
			t.FailNow()
		}
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableRecovEntireLog(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
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
		var ct *ConcTable
		var err error

		// clean state before creating
		if err := cleanAllLogStates(); err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		ct, err = generateRandConcTable(nCmds, wrt, dif, cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		raw, num, err := ct.RecovEntireLog()
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

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableLatencyMeasurementAndSync(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	cfgs := []LogConfig{
		{
			Inmem:        false,
			KeepAll:      true,
			Sync:         true,
			Measure:      true,
			MeasureFname: "./measure.log",
			Tick:         Interval,
			Period:       200,
			Fname:        "./logstate.log",
		},
		{
			Inmem:        false,
			KeepAll:      true,
			Sync:         true,
			Measure:      true,
			MeasureFname: "./measure.log",
			Tick:         Interval,
			Period:       1,
			Fname:        "./logstate.log",
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
		ct, err := generateRandConcTable(nCmds, wrt, dif, cf)
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
		_, err := generateRandConcTable(nCmds, wrt, dif, cf)
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

func generateRandConcTable(n uint64, wrt, dif int, cfg LogConfig) (*ConcTable, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	var ct *ConcTable
	var err error

	ct, err = NewConcTableWithConfig(context.TODO(), defaultConcLvl, &cfg)
	if err != nil {
		return nil, err
	}

	for i := uint64(0); i < n; i++ {
		var cmd pb.Entry
		if cn := r.Intn(100); cn < wrt {
			cmd = pb.Entry{
				Id:      i,
				Key:     int64(r.Intn(dif)),
				WriteOp: true,
				Command: generateRandByteSlice(),
			}

		} else {
			cmd = pb.Entry{Id: i}
		}
		err = ct.Log(&cmd)
		if err != nil {
			return nil, err
		}
	}
	return ct, nil
}

// deserializeRawLog emulates the same procedure implemented by a recoverying
// replica, interpreting the serialized log received from any byte stream.
func deserializeRawLog(log []byte) ([]*pb.Entry, error) {
	rd := bytes.NewReader(log)

	// read the retrieved log interval
	var f, l uint64
	var ln int
	_, err := fmt.Fscanf(rd, "%d\n%d\n%d\n", &f, &l, &ln)
	if err != nil {
		return nil, err
	}

	cmds := make([]*pb.Entry, 0, ln)
	for j := 0; j < ln; j++ {
		var commandLength int32
		err := binary.Read(rd, binary.BigEndian, &commandLength)
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

		c := pb.Entry{}
		if err = proto.Unmarshal(serializedCmd, &c); err != nil {
			return nil, err
		}
		cmds = append(cmds, &c)
	}

	var eol string
	_, err = fmt.Fscanf(rd, "\n%s\n", &eol)
	if err != nil {
		return nil, err
	}

	if eol != "EOL" {
		return nil, fmt.Errorf("expected EOL flag, got '%s'", eol)
	}
	return cmds, nil
}

// deserializeRawLogStream emulates the same procedure implemented by a recov
// replica, interpreting the serialized log stream received from RecovEntireLog
// different calls.
func deserializeRawLogStream(stream []byte, size int) ([]*pb.Entry, error) {
	rd := bytes.NewReader(stream)
	cmds := make([]*pb.Entry, 0, 256*size)

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

			c := pb.Entry{}
			if err = proto.Unmarshal(serializedCmd, &c); err != nil {
				fmt.Println("could not parse")
				return nil, err
			}
			cmds = append(cmds, &c)
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

// logsAreEquivalent checks if two command log sequences are equivalent with
// one another. In this context, two logs 'a' and 'b' are considered 'equivalent'
// iff the deterministic, sequentialy execution of key-value commands on 'b'
// yields the same final state observed on executing 'a'. Two recovered sequences
// from beelog can have commands on different orders, which is safe as long as
// if a log posses a command 'c' on index 'i', no other log records a command 'k'
// on 'i' where 'k' != 'c'.
func logsAreEquivalent(logA, logB []*pb.Entry) bool {
	// not the same size, directly not equivalent
	if len(logA) != len(logB) {
		return false
	}

	// they are already deeply equal, same values on the same positions
	if reflect.DeepEqual(logA, logB) {
		return true
	}

	// apply each log on a hash table, checking if they have the same
	// values for the same keys
	htA := make(map[int64][]byte)
	htB := make(map[int64][]byte)

	for i := range logA {
		htA[logA[i].Key] = logA[i].Command
		htB[logB[i].Key] = logB[i].Command
	}
	return reflect.DeepEqual(htA, htB)
}

// logsAreOnlyDelayed checks wheter two log sequences are only delayed or incoherent
// with each other. In this context, two 'equivalent' logs 'a' and 'b' are considered
// 'delayed' with each other if:
//   (i)  'cA' < 'minB' forall command 'cA' in 'a' AND
//   (ii) 'cB' < 'minA' forall command 'cB' in 'b'
//
// where:
//   -'minA' is the lowest index in the set of 'advanced' commands of 'a' from 'b'
//   -'minB' is the lowest index in the set of 'advanced' commands of 'b' from 'a'
//
// 'advanced' commands:
//  Let 'x' and 'y' be log commands, 'x' is considered 'advanced' from 'y' iff:
//   (i)  'x' and 'y' operate over the same underlying key (i.e. 'x.Key' == 'y.Key') AND
//   (ii) 'x' has a higher index than 'y' (i.e. 'x.Ind' > 'y.Ind')
func logsAreOnlyDelayed(logA, logB []*pb.Entry) bool {
	htA := make(map[int64]uint64)
	htB := make(map[int64]uint64)

	// apply both logs on a hash table
	for i := range logA {
		htA[logA[i].Key] = logA[i].Id

		if i < len(logB) {
			htB[logB[i].Key] = logB[i].Id
		}
	}

	// store only the minor inconsistent index
	var minIndexA, minIndexB uint64

	for kA, indA := range htA {
		// both logs have the same record index for a key 'kA'
		if indA == htB[kA] {
			continue
		}

		if indA > htB[kA] {
			// logA has an advance record for 'kA'
			minIndexA = min(minIndexA, indA)

		} else {
			// logB has an advance record for 'kA'
			minIndexB = min(minIndexB, htB[kA])
		}
	}

	// iterate over B, if any key has a higher index than 'minorIndexA' then its wrong
	for _, ind := range htB {
		if ind > minIndexA {
			return false
		}
	}

	// same for A regarding 'minorIndexB'
	for _, ind := range htA {
		if ind > minIndexB {
			return false
		}
	}
	return true
}

func cleanAllLogStates() error {
	// TODO: think about a more restrictive pattern later
	fs, err := filepath.Glob("./*.log")
	if err != nil {
		return err
	}

	for _, f := range fs {
		err := os.Remove(f)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func generateRandByteSlice() []byte {
	buf := make([]byte, defaultSizeOnCommandBytes)
	rand.Read(buf)
	return buf
}
