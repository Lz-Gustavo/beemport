package beemport

import (
	"os"
	"testing"

	"github.com/Lz-Gustavo/beemport/pb"
)

func BenchmarkSyncUpdateOnLogs(b *testing.B) {
	p, n := uint64(0), uint64(1000)
	lg, err := generateRandLog(n)
	if err != nil {
		b.Fatal("could not generate rand log, err:", err.Error())
	}

	tempDir := b.TempDir()
	testCases := []struct {
		name          string
		createLogFile func() (*os.File, error)
		writeLog      func(fd *os.File, lg []*pb.Entry) error
	}{
		{
			"O_SYNCFlagWithNonBufferedMarshal",
			func() (*os.File, error) {
				fn := tempDir + "/osync-unbuf.log"
				return os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_SYNC, 0644)
			},
			func(fd *os.File, lg []*pb.Entry) error {
				defer fd.Close()
				return MarshalLogIntoWriter(fd, &lg, p, n)
			},
		},
		{
			"O_SYNCFlagWithBufferedMarshal",
			func() (*os.File, error) {
				fn := tempDir + "/osync-buf.log"
				return os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_SYNC, 0644)
			},
			func(fd *os.File, lg []*pb.Entry) error {
				defer fd.Close()
				return MarshalBufferedLogIntoWriter(fd, &lg, p, n)
			},
		},
		{
			"FsyncWithNonBufferedMarshal",
			func() (*os.File, error) {
				fn := tempDir + "/fsync-unbuf.log"
				return os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			},
			func(fd *os.File, lg []*pb.Entry) error {
				defer fd.Close()
				if err := MarshalLogIntoWriter(fd, &lg, p, n); err != nil {
					return err
				}
				return fd.Sync()
			},
		},
		{
			"FsyncWithBufferedMarshal",
			func() (*os.File, error) {
				fn := tempDir + "/fsync-buf.log"
				return os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			},
			func(fd *os.File, lg []*pb.Entry) error {
				defer fd.Close()
				if err := MarshalBufferedLogIntoWriter(fd, &lg, p, n); err != nil {
					return err
				}
				return fd.Sync()
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			lcopy := make([]*pb.Entry, len(lg))
			copy(lcopy, lg)

			b.StartTimer()
			fd, err := tc.createLogFile()
			if err != nil {
				b.Fatal("failed creating log file, err:", err.Error())
			}

			if err = tc.writeLog(fd, lcopy); err != nil {
				b.Fatal("failed logging, err:", err.Error())
			}
			b.StopTimer()
		})
	}
}

func generateRandLog(numCmds uint64) ([]*pb.Entry, error) {
	ct, err := generateRandConcTable(numCmds, 100, int(numCmds), *DefaultLogConfig())
	if err != nil {
		return nil, err
	}

	st := ct.retrieveCurrentViewCopy()
	return generateLogFromTable(&st), nil
}
