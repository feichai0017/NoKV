//go:build linux
// +build linux

package mmap

import (
	"os"
	"testing"
)

func TestMremapAndMunmap(t *testing.T) {
	const size = 8192

	f, err := os.CreateTemp("", "mmap-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	data, err := Mmap(f, true, size)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	if len(data) != size {
		t.Fatalf("unexpected len: %d", len(data))
	}

	// Grow mapping and verify old contents survive.
	data[0] = 0xAA
	expanded, err := Mremap(data, size*2)
	if err != nil {
		t.Fatalf("mremap: %v", err)
	}
	if len(expanded) != size*2 {
		t.Fatalf("unexpected expanded len: %d", len(expanded))
	}
	if expanded[0] != 0xAA {
		t.Fatalf("data lost across mremap, got %x", expanded[0])
	}

	if err := Munmap(expanded); err != nil {
		t.Fatalf("munmap: %v", err)
	}
}
