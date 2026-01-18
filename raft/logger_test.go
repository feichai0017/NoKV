package raft_test

import (
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
)

type stubLogger struct{}

func (l stubLogger) Debug(...any)            {}
func (l stubLogger) Debugf(string, ...any)   {}
func (l stubLogger) Info(...any)             {}
func (l stubLogger) Infof(string, ...any)    {}
func (l stubLogger) Warning(...any)          {}
func (l stubLogger) Warningf(string, ...any) {}
func (l stubLogger) Error(...any)            {}
func (l stubLogger) Errorf(string, ...any)   {}
func (l stubLogger) Fatal(...any)            {}
func (l stubLogger) Fatalf(string, ...any)   {}
func (l stubLogger) Panic(...any)            {}
func (l stubLogger) Panicf(string, ...any)   {}

func TestSetLoggerAndReset(t *testing.T) {
	myraft.SetLogger(stubLogger{})
	myraft.ResetDefaultLogger()
}
