package utils

// Slice holds a reusable buf, will reallocate if you request a larger size than ever before.
// One problem is with n distinct sizes in random order it'll reallocate log(n) times.
type Slice struct {
	buf []byte
}