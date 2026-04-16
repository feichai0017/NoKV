package namespace

import "testing"

func TestEncodeListingDeltaKey(t *testing.T) {
	parent := []byte("/bucket/a/b")
	pageID := []byte("03")
	name := []byte("file-1")

	key := encodeListingDeltaKey(parent, pageID, name)
	want := "LD|/bucket/a/b|03|file-1"
	if string(key) != want {
		t.Fatalf("unexpected delta key: got %q want %q", key, want)
	}
}

func TestEncodePageDeltaKey(t *testing.T) {
	parent := []byte("/bucket/a/b")
	pageID := []byte("rp00000001")

	key := encodePageDeltaLogKey(parent, pageID, 42)
	want := "LDP|/bucket/a/b|rp00000001|#00000000000000000042"
	if string(key) != want {
		t.Fatalf("unexpected page-local delta key: got %q want %q", key, want)
	}
}
