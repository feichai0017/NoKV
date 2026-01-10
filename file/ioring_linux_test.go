//go:build linux
// +build linux

package file

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestIOUringWriteAndFsync(t *testing.T) {
	ring, err := NewIOUring(8, 0)
	if err != nil {
		if errors.Is(err, ErrIOUringUnavailable) ||
			errors.Is(err, unix.ENOSYS) ||
			errors.Is(err, unix.EPERM) ||
			errors.Is(err, unix.EINVAL) ||
			errors.Is(err, unix.EOPNOTSUPP) {
			t.Skipf("io_uring not available: %v", err)
		}
		t.Fatalf("NewIOUring: %v", err)
	}
	defer ring.Close()

	tmp, err := os.CreateTemp("", "nokv_ioring_*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	data := []byte("hello-ioring")
	if err := ring.Submit(IORequest{
		Op:       IOUringOpWrite,
		FD:       int(tmp.Fd()),
		Offset:   0,
		Buffer:   data,
		UserData: 1,
	}); err != nil {
		t.Fatalf("Submit write: %v", err)
	}
	comps, err := ring.Wait(1)
	if err != nil {
		t.Fatalf("Wait write: %v", err)
	}
	if len(comps) == 0 {
		t.Fatalf("missing write completion")
	}
	if comps[0].Result < 0 {
		t.Fatalf("write failed: %v", unix.Errno(-comps[0].Result))
	}
	if comps[0].Result != int32(len(data)) {
		t.Fatalf("write short: %d", comps[0].Result)
	}

	if err := ring.Submit(IORequest{
		Op:       IOUringOpFsync,
		FD:       int(tmp.Fd()),
		UserData: 2,
	}); err != nil {
		t.Fatalf("Submit fsync: %v", err)
	}
	comps, err = ring.Wait(1)
	if err != nil {
		t.Fatalf("Wait fsync: %v", err)
	}
	if len(comps) == 0 {
		t.Fatalf("missing fsync completion")
	}
	if comps[0].Result < 0 {
		t.Fatalf("fsync failed: %v", unix.Errno(-comps[0].Result))
	}

	buf := make([]byte, len(data))
	if _, err := tmp.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf) != string(data) {
		t.Fatalf("data mismatch: got %q want %q", buf, data)
	}
}
