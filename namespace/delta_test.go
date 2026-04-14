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
