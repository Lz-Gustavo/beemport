package beemport

import (
	"bufio"
	"context"
	"os"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beemport/pb"
)

const latencyMeasurementFn = "./measure.log"

func TestMeasurementOnLogAndMeasureLat(t *testing.T) {
	first, n := uint64(0), uint64(100)
	period := uint32(10)
	cfg := &LogConfig{
		Inmem:        false,
		KeepAll:      true,
		Tick:         Interval,
		Period:       period,
		Fname:        "./logstate.log",
		Measure:      true,
		MeasureFname: latencyMeasurementFn,
	}

	ct, err := NewConcTableWithConfig(context.TODO(), defaultConcLvl, cfg)
	if err != nil {
		t.Fatal("could not init conctable structure, err:", err.Error())
	}

	for i := first; i < n; i++ {
		cmd := &pb.Entry{
			Id:      i,
			WriteOp: true,
			Key:     int64(i),
			Command: generateRandByteSlice(),
		}

		if _, err := ct.LogAndMeasureLat(cmd, true); err != nil {
			t.Fatal(err.Error())
		}
	}

	// wait for table persistence
	time.Sleep(time.Second)
	ct.Shutdown()

	count, err := countRecordsOnMeasureFile()
	if err != nil {
		t.Fatal(err.Error())
	}

	if count*int(period) != int(n) {
		t.Fatalf("measured a different number of persisted intervals: %d instead of %d", count*int(period), n)
	}

	if err := cleanAllLogStates(); err != nil {
		t.Fatal(err.Error())
	}
}

func countRecordsOnMeasureFile() (int, error) {
	fd, err := os.Open(latencyMeasurementFn)
	if err != nil {
		return -1, err
	}
	defer fd.Close()

	count := 0
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		count++
	}
	return count, nil
}
