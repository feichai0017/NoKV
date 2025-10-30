package main

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
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
