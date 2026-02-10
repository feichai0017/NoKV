package metrics

import (
	"strings"
	"sync"
	"sync/atomic"
)

// RedisSnapshot captures a point-in-time view of Redis gateway counters.
type RedisSnapshot struct {
	CommandsTotal        uint64            `json:"commands_total"`
	ErrorsTotal          uint64            `json:"errors_total"`
	ConnectionsActive    int64             `json:"connections_active"`
	ConnectionsAccepted  uint64            `json:"connections_accepted"`
	CommandsPerOperation map[string]uint64 `json:"commands_per_operation"`
}

// ExpvarMap converts the snapshot into the legacy expvar-friendly shape.
func (s RedisSnapshot) ExpvarMap() map[string]any {
	return map[string]any{
		"commands_total":         s.CommandsTotal,
		"errors_total":           s.ErrorsTotal,
		"connections_active":     s.ConnectionsActive,
		"connections_accepted":   s.ConnectionsAccepted,
		"commands_per_operation": s.CommandsPerOperation,
	}
}

// RedisMetrics captures lightweight Redis gateway stats.
type RedisMetrics struct {
	commandsTotal       atomic.Uint64
	errorsTotal         atomic.Uint64
	connectionsCurrent  atomic.Int64
	connectionsAccepted atomic.Uint64
	mu                  sync.RWMutex
	commandCounts       map[string]*atomic.Uint64
}

var defaultRedisMetrics atomic.Pointer[RedisMetrics]

func NewRedisMetrics(commandNames []string) *RedisMetrics {
	rm := &RedisMetrics{
		commandCounts: make(map[string]*atomic.Uint64, len(commandNames)),
	}
	for _, name := range commandNames {
		rm.commandCounts[strings.ToUpper(name)] = &atomic.Uint64{}
	}
	return rm
}

// SetDefaultRedisMetrics overrides the process-wide Redis collector.
func SetDefaultRedisMetrics(rm *RedisMetrics) {
	defaultRedisMetrics.Store(rm)
}

// DefaultRedisMetrics returns the process-wide Redis collector.
func DefaultRedisMetrics() *RedisMetrics {
	return defaultRedisMetrics.Load()
}

// DefaultRedisSnapshot returns a snapshot from the process-wide collector.
func DefaultRedisSnapshot() RedisSnapshot {
	if rm := defaultRedisMetrics.Load(); rm != nil {
		return rm.Snapshot()
	}
	return RedisSnapshot{}
}

func (rm *RedisMetrics) Snapshot() RedisSnapshot {
	if rm == nil {
		return RedisSnapshot{}
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	commands := make(map[string]uint64, len(rm.commandCounts))
	for name, counter := range rm.commandCounts {
		commands[name] = counter.Load()
	}

	return RedisSnapshot{
		CommandsTotal:        rm.commandsTotal.Load(),
		ErrorsTotal:          rm.errorsTotal.Load(),
		ConnectionsActive:    rm.connectionsCurrent.Load(),
		ConnectionsAccepted:  rm.connectionsAccepted.Load(),
		CommandsPerOperation: commands,
	}
}

func (rm *RedisMetrics) IncCommand(name string) {
	if rm == nil {
		return
	}
	rm.commandsTotal.Add(1)
	name = strings.ToUpper(name)
	rm.mu.RLock()
	counter := rm.commandCounts[name]
	rm.mu.RUnlock()
	if counter != nil {
		counter.Add(1)
		return
	}
	rm.mu.Lock()
	counter = rm.commandCounts[name]
	if counter == nil {
		counter = &atomic.Uint64{}
		rm.commandCounts[name] = counter
	}
	rm.mu.Unlock()
	counter.Add(1)
}

func (rm *RedisMetrics) IncError() {
	if rm == nil {
		return
	}
	rm.errorsTotal.Add(1)
}

func (rm *RedisMetrics) ConnOpened() {
	if rm == nil {
		return
	}
	rm.connectionsAccepted.Add(1)
	rm.connectionsCurrent.Add(1)
}

func (rm *RedisMetrics) ConnClosed() {
	if rm == nil {
		return
	}
	rm.connectionsCurrent.Add(-1)
}
