package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
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
	defer db.Close()

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
	defer conn.Close()

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
	mget      []*redisValue
	msetErr   error
	exists    int64
	incrValue int64
	incrErr   error
}

func (b *stubBackend) Get([]byte) (*redisValue, error)               { return nil, nil }
func (b *stubBackend) Set(setArgs) (bool, error)                     { return false, nil }
func (b *stubBackend) Del([][]byte) (int64, error)                   { return 0, nil }
func (b *stubBackend) MGet(keys [][]byte) ([]*redisValue, error)     { return b.mget, nil }
func (b *stubBackend) MSet(pairs [][2][]byte) error                  { return b.msetErr }
func (b *stubBackend) Exists(keys [][]byte) (int64, error)           { return b.exists, nil }
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
