package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/metrics"
)

const (
	flushCommandBatch  = 8
	flushBufferedBytes = 2048
)

var (
	metricsOnce     sync.Once
	metricsInstance *metrics.RedisMetrics
)

func globalRedisMetrics() *metrics.RedisMetrics {
	metricsOnce.Do(func() {
		commands := []string{
			"PING", "ECHO", "GET", "SET", "DEL", "MGET", "MSET",
			"INCR", "DECR", "INCRBY", "DECRBY", "EXISTS", "QUIT",
		}
		metricsInstance = metrics.NewRedisMetrics(commands)
		metrics.SetDefaultRedisMetrics(metricsInstance)
	})
	return metricsInstance
}

type redisServer struct {
	backend redisBackend
	wg      sync.WaitGroup
	metrics *metrics.RedisMetrics
}

func newServer(backend redisBackend) *redisServer {
	return &redisServer{
		backend: backend,
		metrics: globalRedisMetrics(),
	}
}

func (s *redisServer) Serve(ln net.Listener) error {
	var tempDelay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if isRetryableAcceptError(err) {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			if errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection") {
				s.Wait()
				return nil
			}
			s.Wait()
			return err
		}
		tempDelay = 0
		s.metrics.ConnOpened()
		s.wg.Go(func() {
			s.handleConn(conn)
		})
	}
}

func (s *redisServer) Wait() {
	s.wg.Wait()
}

var errQuit = errors.New("client quit")

func isRetryableAcceptError(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Timeout() {
			return true
		}
		if opErr.Op == "accept" && (errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ECONNRESET)) {
			return true
		}
	}
	return false
}

func (s *redisServer) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	defer s.metrics.ConnClosed()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	pending := 0

	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		args, err := parseRESP(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				_ = s.respondError(writer, "timeout")
				_ = writer.Flush()
				return
			}
			_ = s.respondError(writer, err.Error())
			_ = writer.Flush()
			return
		}
		if len(args) == 0 {
			if pending > 0 {
				if err := writer.Flush(); err != nil {
					return
				}
				pending = 0
			}
			continue
		}
		if err := s.execute(writer, args); err != nil {
			if errors.Is(err, errQuit) {
				_ = writer.Flush()
				return
			}
			_ = s.respondError(writer, err.Error())
		}
		pending++
		flushNow := false
		if reader.Buffered() == 0 {
			flushNow = true
		} else if pending >= flushCommandBatch || writer.Buffered() >= flushBufferedBytes {
			flushNow = true
		}
		if flushNow {
			if err := writer.Flush(); err != nil {
				return
			}
			pending = 0
		}
	}
}

func (s *redisServer) execute(w *bufio.Writer, args [][]byte) error {
	cmd := strings.ToUpper(string(args[0]))
	s.metrics.IncCommand(cmd)
	switch cmd {
	case "PING":
		if len(args) > 1 && len(args[1]) > 0 {
			return writeBulk(w, args[1])
		}
		return writeSimpleString(w, "PONG")
	case "ECHO":
		if len(args) != 2 {
			return s.respondError(w, "wrong number of arguments for 'ECHO'")
		}
		return writeBulk(w, args[1])
	case "GET":
		if len(args) != 2 {
			return s.respondError(w, "wrong number of arguments for 'GET'")
		}
		return s.execGet(w, args[1])
	case "SET":
		if len(args) < 3 {
			return s.respondError(w, "wrong number of arguments for 'SET'")
		}
		return s.execSet(w, args[1:])
	case "DEL":
		if len(args) < 2 {
			return s.respondError(w, "wrong number of arguments for 'DEL'")
		}
		return s.execDel(w, args[1:])
	case "MGET":
		if len(args) < 2 {
			return s.respondError(w, "wrong number of arguments for 'MGET'")
		}
		return s.execMGet(w, args[1:])
	case "MSET":
		if len(args) < 3 || len(args[1:])%2 != 0 {
			return s.respondError(w, "wrong number of arguments for 'MSET'")
		}
		return s.execMSet(w, args[1:])
	case "INCR":
		if len(args) != 2 {
			return s.respondError(w, "wrong number of arguments for 'INCR'")
		}
		return s.execIncrBy(w, args[1], 1)
	case "DECR":
		if len(args) != 2 {
			return s.respondError(w, "wrong number of arguments for 'DECR'")
		}
		return s.execIncrBy(w, args[1], -1)
	case "INCRBY":
		if len(args) != 3 {
			return s.respondError(w, "wrong number of arguments for 'INCRBY'")
		}
		delta, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return s.respondError(w, errNotIntegerMsg)
		}
		return s.execIncrBy(w, args[1], delta)
	case "DECRBY":
		if len(args) != 3 {
			return s.respondError(w, "wrong number of arguments for 'DECRBY'")
		}
		delta, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return s.respondError(w, errNotIntegerMsg)
		}
		return s.execIncrBy(w, args[1], -delta)
	case "EXISTS":
		if len(args) < 2 {
			return s.respondError(w, "wrong number of arguments for 'EXISTS'")
		}
		return s.execExists(w, args[1:])
	case "QUIT":
		if err := writeSimpleString(w, "OK"); err != nil {
			return err
		}
		return errQuit
	default:
		return s.respondError(w, "unknown command '"+strings.ToLower(cmd)+"'")
	}
}

func (s *redisServer) execGet(w *bufio.Writer, key []byte) error {
	val, err := s.backend.Get(key)
	if err != nil {
		return err
	}
	if val == nil || !val.Found {
		return writeNil(w)
	}
	return writeBulk(w, val.Value)
}

func (s *redisServer) execSet(w *bufio.Writer, args [][]byte) error {
	key := args[0]
	value := args[1]

	var (
		nx        bool
		xx        bool
		expireAt  uint64
		hasExpire bool
	)

	for i := 2; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "NX":
			if xx {
				return s.respondError(w, "syntax error")
			}
			nx = true
			i++
		case "XX":
			if nx {
				return s.respondError(w, "syntax error")
			}
			xx = true
			i++
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpire {
				return s.respondError(w, "syntax error")
			}
			if i+1 >= len(args) {
				return s.respondError(w, "syntax error")
			}
			num, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return s.respondError(w, "value is not an integer or out of range")
			}
			if num <= 0 {
				return s.respondError(w, "invalid expire time in set")
			}
			switch opt {
			case "EX":
				now := time.Now()
				expireAt = uint64(now.Add(time.Duration(num) * time.Second).Unix())
				if expireAt <= uint64(now.Unix()) {
					expireAt = uint64(now.Add(time.Second).Unix())
				}
			case "PX":
				now := time.Now()
				expireAt = uint64(now.Add(time.Duration(num) * time.Millisecond).Unix())
				if expireAt <= uint64(now.Unix()) {
					expireAt = uint64(now.Add(time.Second).Unix())
				}
			case "EXAT":
				expireAt = uint64(num)
			case "PXAT":
				sec := num / 1000
				nsec := (num % 1000) * int64(time.Millisecond)
				expireAt = uint64(time.Unix(sec, nsec).Unix())
			}
			if expireAt == 0 {
				return s.respondError(w, "invalid expire time in set")
			}
			hasExpire = true
			i += 2
		case "KEEPTTL":
			// not yet supported
			return s.respondError(w, "syntax error")
		default:
			return s.respondError(w, "syntax error")
		}
	}

	ok, err := s.backend.Set(setArgs{
		Key:      key,
		Value:    value,
		NX:       nx,
		XX:       xx,
		ExpireAt: expireAt,
	})
	if err != nil {
		switch {
		case errors.Is(err, errConditionNotMet):
			return writeNil(w)
		case errors.Is(err, errUnsupported):
			return s.respondError(w, err.Error())
		default:
			return err
		}
	}
	if ok {
		return writeSimpleString(w, "OK")
	}
	return writeNil(w)
}

func (s *redisServer) execDel(w *bufio.Writer, keys [][]byte) error {
	removed, err := s.backend.Del(keys)
	if err != nil {
		return err
	}
	return writeInteger(w, removed)
}

func (s *redisServer) execMGet(w *bufio.Writer, keys [][]byte) error {
	vals, err := s.backend.MGet(keys)
	if err != nil {
		return err
	}
	results := make([][]byte, len(vals))
	for i, val := range vals {
		if val == nil || !val.Found {
			results[i] = nil
			continue
		}
		results[i] = val.Value
	}
	return writeArray(w, results)
}

func (s *redisServer) execMSet(w *bufio.Writer, args [][]byte) error {
	pairs := make([][2][]byte, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs[i/2] = [2][]byte{args[i], args[i+1]}
	}
	if err := s.backend.MSet(pairs); err != nil {
		if errors.Is(err, errUnsupported) {
			return s.respondError(w, err.Error())
		}
		return err
	}
	return writeSimpleString(w, "OK")
}

var (
	errNotInteger    = errors.New("value is not an integer or out of range")
	errOverflow      = errors.New("increment or decrement would overflow")
	errNotIntegerMsg = "ERR value is not an integer or out of range"
	errOverflowMsg   = "ERR increment or decrement would overflow"
)

func (s *redisServer) execIncrBy(w *bufio.Writer, key []byte, delta int64) error {
	result, err := s.backend.IncrBy(key, delta)
	if err != nil {
		switch {
		case errors.Is(err, errNotInteger):
			return s.respondError(w, errNotIntegerMsg)
		case errors.Is(err, errOverflow):
			return s.respondError(w, errOverflowMsg)
		case errors.Is(err, errUnsupported):
			return s.respondError(w, err.Error())
		default:
			return err
		}
	}
	return writeInteger(w, result)
}

func (s *redisServer) execExists(w *bufio.Writer, keys [][]byte) error {
	count, err := s.backend.Exists(keys)
	if err != nil {
		return err
	}
	return writeInteger(w, count)
}

func parseRESP(r *bufio.Reader) ([][]byte, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '*':
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid multibulk length %q", line)
		}
		if n < 0 {
			return nil, nil
		}
		out := make([][]byte, 0, n)
		for range n {
			b, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			if b != '$' {
				return nil, fmt.Errorf("expected bulk string")
			}
			line, err := readLine(r)
			if err != nil {
				return nil, err
			}
			l, err := strconv.Atoi(line)
			if err != nil {
				return nil, fmt.Errorf("invalid bulk length %q", line)
			}
			if l < 0 {
				out = append(out, nil)
				continue
			}
			buf := make([]byte, l)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			if err := expectCRLF(r); err != nil {
				return nil, err
			}
			out = append(out, buf)
		}
		return out, nil
	default:
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if line == "" {
			return nil, nil
		}
		fields := strings.Fields(line)
		out := make([][]byte, len(fields))
		for i, f := range fields {
			out[i] = []byte(f)
		}
		return out, nil
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("invalid line terminator")
	}
	return line[:len(line)-2], nil
}

func expectCRLF(r *bufio.Reader) error {
	b1, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b1 != '\r' {
		return fmt.Errorf("expected CR")
	}
	b2, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b2 != '\n' {
		return fmt.Errorf("expected LF")
	}
	return nil
}

func writeSimpleString(w *bufio.Writer, msg string) error {
	_, err := w.WriteString("+" + msg + "\r\n")
	return err
}

func writeBulk(w *bufio.Writer, data []byte) error {
	if data == nil {
		return writeNil(w)
	}
	if _, err := w.WriteString("$" + strconv.Itoa(len(data)) + "\r\n"); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	_, err := w.WriteString("\r\n")
	return err
}

func writeNil(w *bufio.Writer) error {
	_, err := w.WriteString("$-1\r\n")
	return err
}

func writeInteger(w *bufio.Writer, v int64) error {
	if _, err := w.WriteString(":" + strconv.FormatInt(v, 10) + "\r\n"); err != nil {
		return err
	}
	return nil
}

func writeArray(w *bufio.Writer, values [][]byte) error {
	if values == nil {
		_, err := w.WriteString("*-1\r\n")
		return err
	}
	if _, err := w.WriteString("*" + strconv.Itoa(len(values)) + "\r\n"); err != nil {
		return err
	}
	for _, val := range values {
		if err := writeBulk(w, val); err != nil {
			return err
		}
	}
	return nil
}

func writeError(w *bufio.Writer, msg string) error {
	if !strings.HasPrefix(msg, "ERR") && !strings.HasPrefix(msg, "WRONG") {
		msg = "ERR " + msg
	}
	_, err := w.WriteString("-" + msg + "\r\n")
	return err
}

func (s *redisServer) respondError(w *bufio.Writer, msg string) error {
	if s.metrics != nil {
		s.metrics.IncError()
	}
	return writeError(w, msg)
}
