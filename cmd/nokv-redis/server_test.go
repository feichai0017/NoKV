package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/stretchr/testify/require"
)

func TestRedisGatewayBasicCommands(t *testing.T) {
	dir := t.TempDir()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueThreshold = 0
	opt.MaxBatchCount = 1024
	opt.MaxBatchSize = 16 << 20

	db := NoKV.Open(opt)
	defer func() { _ = db.Close() }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	backend := newEmbeddedBackend(db)
	server := newServer(backend)
	go func() {
		_ = server.Serve(ln)
	}()
	t.Cleanup(func() {
		_ = backend.Close()
		_ = ln.Close()
		server.Wait()
	})

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)

	send := func(cmd string) string {
		if _, err := fmt.Fprintf(conn, "%s", cmd); err != nil {
			t.Fatalf("write: %v", err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read line: %v", err)
		}
		if len(resp) == 0 {
			t.Fatalf("empty response")
		}
		if resp[0] == '$' {
			// bulk string, read payload
			var size int
			if _, err := fmt.Sscanf(resp, "$%d\r\n", &size); err != nil {
				t.Fatalf("parse bulk: %v", err)
			}
			if size >= 0 {
				buf := make([]byte, size+2)
				if _, err := reader.Read(buf); err != nil {
					t.Fatalf("read bulk body: %v", err)
				}
				resp = resp + string(buf)
			}
		} else if resp[0] == '*' {
			var count int
			if _, err := fmt.Sscanf(resp, "*%d\r\n", &count); err != nil {
				t.Fatalf("parse array: %v", err)
			}
			for i := 0; i < count; i++ {
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Fatalf("read array elem header: %v", err)
				}
				resp += line
				if len(line) == 0 {
					t.Fatalf("empty array element header")
				}
				switch line[0] {
				case '$':
					var size int
					if _, err := fmt.Sscanf(line, "$%d\r\n", &size); err != nil {
						t.Fatalf("parse array bulk len: %v", err)
					}
					if size >= 0 {
						buf := make([]byte, size+2)
						if _, err := reader.Read(buf); err != nil {
							t.Fatalf("read array bulk body: %v", err)
						}
						resp += string(buf)
					}
				case ':', '+', '-':
					// already read full line
				default:
					t.Fatalf("unexpected array element prefix %q", line[0])
				}
			}
		} else if resp[0] != '+' && resp[0] != '-' && resp[0] != ':' {
			t.Fatalf("unexpected response prefix %q", resp[0])
		}
		return resp
	}

	if got := send("*1\r\n$4\r\nPING\r\n"); got != "+PONG\r\n" {
		t.Fatalf("PING: got %q", got)
	}
	if got := send("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET: got %q", got)
	}
	if got := send("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"); got != "$3\r\nbar\r\n" {
		t.Fatalf("GET: got %q", got)
	}
	if got := send("*2\r\n$4\r\nMGET\r\n$3\r\nfoo\r\n"); got != "*1\r\n$3\r\nbar\r\n" {
		t.Fatalf("MGET: got %q", got)
	}
	if got := send("*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n"); got != ":1\r\n" {
		t.Fatalf("DEL: got %q", got)
	}
	if got := send("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"); got != "$-1\r\n" {
		t.Fatalf("GET missing: got %q", got)
	}
	if got := send("*4\r\n$3\r\nSET\r\n$3\r\nnxk\r\n$3\r\none\r\n$2\r\nNX\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET NX: got %q", got)
	}
	if got := send("*2\r\n$3\r\nGET\r\n$3\r\nnxk\r\n"); got != "$3\r\none\r\n" {
		t.Fatalf("GET after NX set: got %q", got)
	}
	if got := send("*4\r\n$3\r\nSET\r\n$3\r\nnxk\r\n$3\r\ntwo\r\n$2\r\nNX\r\n"); got != "$-1\r\n" {
		t.Fatalf("SET NX should fail: got %q", got)
	}
	if got := send("*4\r\n$3\r\nSET\r\n$4\r\nxxky\r\n$3\r\none\r\n$2\r\nXX\r\n"); got != "$-1\r\n" {
		t.Fatalf("SET XX without key: got %q", got)
	}
	if got := send("*3\r\n$3\r\nSET\r\n$4\r\nxxky\r\n$3\r\none\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET base for XX: got %q", got)
	}
	if got := send("*2\r\n$3\r\nGET\r\n$4\r\nxxky\r\n"); got != "$3\r\none\r\n" {
		t.Fatalf("GET after base SET: got %q", got)
	}
	if got := send("*4\r\n$3\r\nSET\r\n$4\r\nxxky\r\n$3\r\ntwo\r\n$2\r\nXX\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET XX with existing key: got %q", got)
	}
	if got := send("*5\r\n$3\r\nSET\r\n$4\r\nexp1\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n1\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET EX: got %q", got)
	}
	time.Sleep(1100 * time.Millisecond)
	if got := send("*2\r\n$3\r\nGET\r\n$4\r\nexp1\r\n"); got != "$-1\r\n" {
		t.Fatalf("GET after EX expire: got %q", got)
	}
	if got := send("*5\r\n$3\r\nSET\r\n$4\r\nexp2\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\n500\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET PX: got %q", got)
	}
	time.Sleep(1100 * time.Millisecond)
	if got := send("*2\r\n$3\r\nGET\r\n$4\r\nexp2\r\n"); got != "$-1\r\n" {
		t.Fatalf("GET after PX expire: got %q", got)
	}
	if got := send("*2\r\n$4\r\nINCR\r\n$4\r\nctr1\r\n"); got != ":1\r\n" {
		t.Fatalf("INCR new key: got %q", got)
	}
	if got := send("*2\r\n$4\r\nINCR\r\n$4\r\nctr1\r\n"); got != ":2\r\n" {
		t.Fatalf("INCR existing key: got %q", got)
	}
	if got := send("*2\r\n$3\r\nGET\r\n$4\r\nctr1\r\n"); got != "$1\r\n2\r\n" {
		t.Fatalf("GET after INCR: got %q", got)
	}
	if got := send("*3\r\n$6\r\nINCRBY\r\n$4\r\nctr1\r\n$2\r\n10\r\n"); got != ":12\r\n" {
		t.Fatalf("INCRBY: got %q", got)
	}
	if got := send("*2\r\n$4\r\nDECR\r\n$4\r\nctr1\r\n"); got != ":11\r\n" {
		t.Fatalf("DECR: got %q", got)
	}
	if got := send("*3\r\n$6\r\nDECRBY\r\n$4\r\nctr1\r\n$1\r\n2\r\n"); got != ":9\r\n" {
		t.Fatalf("DECRBY: got %q", got)
	}
	if got := send("*3\r\n$3\r\nSET\r\n$5\r\nplain\r\n$3\r\nfoo\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET plain for INCR error: got %q", got)
	}
	if got := send("*2\r\n$4\r\nINCR\r\n$5\r\nplain\r\n"); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("INCR non-integer: got %q", got)
	}
	if got := send("*3\r\n$3\r\nSET\r\n$6\r\nbigkey\r\n$19\r\n9223372036854775807\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET max int: got %q", got)
	}
	if got := send("*2\r\n$4\r\nINCR\r\n$6\r\nbigkey\r\n"); got != "-ERR increment or decrement would overflow\r\n" {
		t.Fatalf("INCR overflow: got %q", got)
	}
	if got := send("*5\r\n$3\r\nSET\r\n$4\r\nkeep\r\n$1\r\n0\r\n$2\r\nEX\r\n$1\r\n1\r\n"); got != "+OK\r\n" {
		t.Fatalf("SET with expire for TTL retention: got %q", got)
	}
	if got := send("*2\r\n$4\r\nINCR\r\n$4\r\nkeep\r\n"); got != ":1\r\n" {
		t.Fatalf("INCR with TTL: got %q", got)
	}
	time.Sleep(1200 * time.Millisecond)
	if got := send("*2\r\n$3\r\nGET\r\n$4\r\nkeep\r\n"); got != "$-1\r\n" {
		t.Fatalf("GET after TTL retention check: got %q", got)
	}
	if got := send("*1\r\n$4\r\nQUIT\r\n"); got != "+OK\r\n" {
		t.Fatalf("QUIT: got %q", got)
	}
}

func TestParseRESPInline(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("PING\r\n"))
	out, err := parseRESP(r)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("PING")}, out)
}

func TestParseRESPArray(t *testing.T) {
	payload := "*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n"
	r := bufio.NewReader(strings.NewReader(payload))
	out, err := parseRESP(r)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("PING"), []byte("PONG")}, out)
}

func TestParseRESPNilArray(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*-1\r\n"))
	out, err := parseRESP(r)
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestParseRESPInvalidMultiBulk(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*bad\r\n"))
	_, err := parseRESP(r)
	require.Error(t, err)
}

func TestParseRESPInvalidBulkPrefix(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*1\r\n+OK\r\n"))
	_, err := parseRESP(r)
	require.Error(t, err)
}

func TestReadLineInvalidTerminator(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("oops\n"))
	_, err := readLine(r)
	require.Error(t, err)
}

func TestExpectCRLFFailure(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("x\n"))
	err := expectCRLF(r)
	require.Error(t, err)
}

func TestWriteArray(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	require.NoError(t, writeArray(w, nil))
	require.NoError(t, w.Flush())
	require.Equal(t, "*-1\r\n", buf.String())

	buf.Reset()
	w.Reset(&buf)
	require.NoError(t, writeArray(w, [][]byte{[]byte("a"), []byte("b")}))
	require.NoError(t, w.Flush())
	require.Equal(t, "*2\r\n$1\r\na\r\n$1\r\nb\r\n", buf.String())
}

type stubBackend struct {
	getVal    *redisValue
	getErr    error
	setOK     bool
	setErr    error
	delCount  int64
	delErr    error
	mget      []*redisValue
	mgetErr   error
	msetErr   error
	exists    int64
	existsErr error
	incrValue int64
	incrErr   error
}

func (b *stubBackend) Get([]byte) (*redisValue, error)               { return b.getVal, b.getErr }
func (b *stubBackend) Set(setArgs) (bool, error)                     { return b.setOK, b.setErr }
func (b *stubBackend) Del([][]byte) (int64, error)                   { return b.delCount, b.delErr }
func (b *stubBackend) MGet(keys [][]byte) ([]*redisValue, error)     { return b.mget, b.mgetErr }
func (b *stubBackend) MSet(pairs [][2][]byte) error                  { return b.msetErr }
func (b *stubBackend) Exists(keys [][]byte) (int64, error)           { return b.exists, b.existsErr }
func (b *stubBackend) IncrBy(key []byte, delta int64) (int64, error) { return b.incrValue, b.incrErr }
func (b *stubBackend) Close() error                                  { return nil }

func TestExecMGetAndExists(t *testing.T) {
	backend := &stubBackend{
		mget: []*redisValue{
			{Value: []byte("foo"), Found: true},
			{Found: false},
		},
		exists: 1,
	}
	server := newServer(backend)

	var buf strings.Builder
	writer := bufio.NewWriter(&buf)
	require.NoError(t, server.execMGet(writer, [][]byte{[]byte("k1"), []byte("k2")}))
	require.NoError(t, writer.Flush())
	require.Equal(t, "*2\r\n$3\r\nfoo\r\n$-1\r\n", buf.String())

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execExists(writer, [][]byte{[]byte("k1")}))
	require.NoError(t, writer.Flush())
	require.Equal(t, ":1\r\n", buf.String())
}

func TestExecMSetAndIncrErrors(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)

	var buf strings.Builder
	writer := bufio.NewWriter(&buf)
	require.NoError(t, server.execMSet(writer, [][]byte{[]byte("k1"), []byte("v1")}))
	require.NoError(t, writer.Flush())
	require.Equal(t, "+OK\r\n", buf.String())

	buf.Reset()
	writer.Reset(&buf)
	backend.incrErr = errNotInteger
	require.NoError(t, server.execIncrBy(writer, []byte("k1"), 1))
	require.NoError(t, writer.Flush())
	require.Equal(t, "-"+errNotIntegerMsg+"\r\n", buf.String())

	buf.Reset()
	writer.Reset(&buf)
	backend.incrErr = errOverflow
	require.NoError(t, server.execIncrBy(writer, []byte("k1"), 1))
	require.NoError(t, writer.Flush())
	require.Equal(t, "-"+errOverflowMsg+"\r\n", buf.String())
}

func TestIsRetryableAcceptError(t *testing.T) {
	require.False(t, isRetryableAcceptError(nil))

	timeoutErr := &net.OpError{Op: "accept", Err: timeoutError{}}
	require.True(t, isRetryableAcceptError(timeoutErr))

	connReset := &net.OpError{Op: "accept", Err: syscall.ECONNRESET}
	require.True(t, isRetryableAcceptError(connReset))

	connAbort := &net.OpError{Op: "accept", Err: syscall.ECONNABORTED}
	require.True(t, isRetryableAcceptError(connAbort))

	otherErr := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	require.False(t, isRetryableAcceptError(otherErr))
}

func TestServeRetryableErrorAndClose(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	ln := &errorListener{
		errs: []error{
			&net.OpError{Op: "accept", Err: timeoutError{}},
			net.ErrClosed,
		},
	}
	require.NoError(t, server.Serve(ln))
}

func TestServeNonRetryableError(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	ln := &errorListener{
		errs: []error{errors.New("boom")},
	}
	require.Error(t, server.Serve(ln))
}

func TestHandleConnEOF(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	conn := newScriptedConn(nil)
	server.handleConn(conn)
}

func TestHandleConnParseError(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	conn := newScriptedConn([]byte("*1\r\n+OK\r\n"))
	server.handleConn(conn)
	require.Contains(t, conn.out.String(), "-ERR")
}

func TestHandleConnFlushOnEmptyLine(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	conn := newScriptedConn([]byte("PING\r\n\r\n"))
	server.handleConn(conn)
	require.Contains(t, conn.out.String(), "+PONG")
}

func TestHandleConnTimeout(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	conn := &timeoutConn{}
	server.handleConn(conn)
	require.Contains(t, conn.out.String(), "-ERR timeout")
}

func TestExecuteInvalidArgs(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)
	cases := []struct {
		args [][]byte
		want string
	}{
		{[][]byte{[]byte("ECHO")}, "-ERR"},
		{[][]byte{[]byte("GET")}, "-ERR"},
		{[][]byte{[]byte("SET"), []byte("k")}, "-ERR"},
		{[][]byte{[]byte("DEL")}, "-ERR"},
		{[][]byte{[]byte("MGET")}, "-ERR"},
		{[][]byte{[]byte("MSET"), []byte("k")}, "-ERR"},
		{[][]byte{[]byte("INCR")}, "-ERR"},
		{[][]byte{[]byte("DECR")}, "-ERR"},
		{[][]byte{[]byte("INCRBY"), []byte("k")}, "-ERR"},
		{[][]byte{[]byte("DECRBY"), []byte("k")}, "-ERR"},
		{[][]byte{[]byte("EXISTS")}, "-ERR"},
		{[][]byte{[]byte("UNKNOWN")}, "-ERR"},
	}
	for _, tc := range cases {
		var buf strings.Builder
		writer := bufio.NewWriter(&buf)
		require.NoError(t, server.execute(writer, tc.args))
		require.NoError(t, writer.Flush())
		require.Contains(t, buf.String(), tc.want)
	}

	var buf strings.Builder
	writer := bufio.NewWriter(&buf)
	require.NoError(t, server.execute(writer, [][]byte{[]byte("INCRBY"), []byte("k"), []byte("nope")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), errNotIntegerMsg)
}

func TestExecSetErrorsAndOptions(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)

	var buf strings.Builder
	writer := bufio.NewWriter(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("NX"), []byte("XX")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "-ERR")

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("EX")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "-ERR")

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("EX"), []byte("bad")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "value is not an integer")

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("EX"), []byte("0")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "invalid expire time")

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("KEEPTTL")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "syntax error")

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("BAD")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "syntax error")

	buf.Reset()
	writer.Reset(&buf)
	backend.setOK = true
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("EXAT"), []byte("123")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "+OK")

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v"), []byte("PXAT"), []byte("1000")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "+OK")
}

func TestExecSetBackendErrors(t *testing.T) {
	backend := &stubBackend{}
	server := newServer(backend)

	var buf strings.Builder
	writer := bufio.NewWriter(&buf)
	backend.setErr = errConditionNotMet
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v")}))
	require.NoError(t, writer.Flush())
	require.Equal(t, "$-1\r\n", buf.String())

	buf.Reset()
	writer.Reset(&buf)
	backend.setErr = errUnsupported
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v")}))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "-ERR")

	backend.setErr = errors.New("boom")
	err := server.execSet(writer, [][]byte{[]byte("k"), []byte("v")})
	require.Error(t, err)

	buf.Reset()
	writer.Reset(&buf)
	backend.setErr = nil
	backend.setOK = false
	require.NoError(t, server.execSet(writer, [][]byte{[]byte("k"), []byte("v")}))
	require.NoError(t, writer.Flush())
	require.Equal(t, "$-1\r\n", buf.String())
}

func TestExecBackendErrors(t *testing.T) {
	backend := &stubBackend{
		getErr:    errors.New("get"),
		delErr:    errors.New("del"),
		mgetErr:   errors.New("mget"),
		msetErr:   errUnsupported,
		existsErr: errors.New("exists"),
		incrErr:   errUnsupported,
	}
	server := newServer(backend)
	writer := bufio.NewWriter(io.Discard)

	require.Error(t, server.execGet(writer, []byte("k")))
	require.Error(t, server.execDel(writer, [][]byte{[]byte("k")}))
	require.Error(t, server.execMGet(writer, [][]byte{[]byte("k")}))
	require.NoError(t, server.execMSet(writer, [][]byte{[]byte("k"), []byte("v")}))
	require.Error(t, server.execExists(writer, [][]byte{[]byte("k")}))

	var buf strings.Builder
	writer = bufio.NewWriter(&buf)
	require.NoError(t, server.execIncrBy(writer, []byte("k"), 1))
	require.NoError(t, writer.Flush())
	require.Contains(t, buf.String(), "-ERR")
}

func TestParseRESPAdditionalCases(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*1\r\n$bad\r\n"))
	_, err := parseRESP(r)
	require.Error(t, err)

	r = bufio.NewReader(strings.NewReader("*1\r\n$-1\r\n"))
	out, err := parseRESP(r)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Nil(t, out[0])

	r = bufio.NewReader(strings.NewReader("*1\r\n$3\r\nhi\r\n"))
	_, err = parseRESP(r)
	require.Error(t, err)

	r = bufio.NewReader(strings.NewReader("*1\r\n$1\r\na\n"))
	_, err = parseRESP(r)
	require.Error(t, err)

	r = bufio.NewReader(strings.NewReader("\r\n"))
	out, err = parseRESP(r)
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestReadLineError(t *testing.T) {
	r := bufio.NewReader(errReader{})
	_, err := readLine(r)
	require.Error(t, err)
}

func TestExpectCRLFSecondByteError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("\r"))
	err := expectCRLF(r)
	require.Error(t, err)
}

func TestWriteHelpersErrors(t *testing.T) {
	w := bufio.NewWriterSize(errWriter{}, 1)
	require.Error(t, writeInteger(w, 1))
	require.Error(t, writeBulk(w, []byte("data")))
	require.Error(t, writeArray(w, [][]byte{[]byte("a")}))

	var buf strings.Builder
	writer := bufio.NewWriter(&buf)
	require.NoError(t, writeError(writer, "oops"))
	require.NoError(t, writer.Flush())
	require.Equal(t, "-ERR oops\r\n", buf.String())

	buf.Reset()
	writer.Reset(&buf)
	require.NoError(t, writeError(writer, "WRONG nope"))
	require.NoError(t, writer.Flush())
	require.Equal(t, "-WRONG nope\r\n", buf.String())
}

type errorListener struct {
	errs []error
}

func (l *errorListener) Accept() (net.Conn, error) {
	if len(l.errs) == 0 {
		return nil, net.ErrClosed
	}
	err := l.errs[0]
	l.errs = l.errs[1:]
	return nil, err
}

func (l *errorListener) Close() error { return nil }
func (l *errorListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

type scriptedConn struct {
	reader *bytes.Reader
	out    bytes.Buffer
}

func newScriptedConn(data []byte) *scriptedConn {
	return &scriptedConn{reader: bytes.NewReader(data)}
}

func (c *scriptedConn) Read(p []byte) (int, error)       { return c.reader.Read(p) }
func (c *scriptedConn) Write(p []byte) (int, error)      { return c.out.Write(p) }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type timeoutConn struct {
	out bytes.Buffer
}

func (c *timeoutConn) Read(p []byte) (int, error)       { return 0, timeoutError{} }
func (c *timeoutConn) Write(p []byte) (int, error)      { return c.out.Write(p) }
func (c *timeoutConn) Close() error                     { return nil }
func (c *timeoutConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *timeoutConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *timeoutConn) SetDeadline(time.Time) error      { return nil }
func (c *timeoutConn) SetReadDeadline(time.Time) error  { return nil }
func (c *timeoutConn) SetWriteDeadline(time.Time) error { return nil }

type errReader struct{}

func (errReader) Read(p []byte) (int, error) { return 0, io.EOF }

type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) { return 0, errors.New("write") }
