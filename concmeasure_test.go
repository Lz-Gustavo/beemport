package beemport

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beemport/pb"
)

func TestLatencyMeasurementV2(t *testing.T) {
	first := uint64(0)
	n := uint64(100)
	cfg := &LogConfig{
		Inmem:        false,
		KeepAll:      true,
		Tick:         Interval,
		Period:       10,
		Fname:        "./logstate.log",
		Measure:      true,
		MeasureFname: "./measure.log",
	}

	ct, err := NewConcTableWithConfig(context.TODO(), 2, cfg)
	if err != nil {
		t.Fatal("could not init conctable structure, err:", err.Error())
	}

	for i := first; i < n; i++ {
		cmd := &pb.Entry{
			Id:      i,
			WriteOp: true,
			Key:     strconv.Itoa(int(i)),
			Command: generateRandByteSlice(),
		}

		if err := ct.LogAndMeasureLat(cmd, true); err != nil {
			t.Fatal(err.Error())
		}
	}

	// wait for table persistence
	time.Sleep(time.Second)

	if ct.latMeasure.counter < int(n) {
		t.Fatalf("measured a different number of logged commands: %d instead of %d", ct.latMeasure.counter, n)
	}

	if ct.latMeasure.fillState != ct.latMeasure.counter {
		t.Fatalf("different fill state observed: %d instead of %d", ct.latMeasure.fillState, ct.latMeasure.counter)
	}

	if ct.latMeasure.perstState != ct.latMeasure.counter {
		t.Fatalf("different perst state observed: %d instead of %d", ct.latMeasure.perstState, ct.latMeasure.counter)
	}

	if err := cleanAllLogStates(); err != nil {
		t.Fatal(err.Error())
	}
}
