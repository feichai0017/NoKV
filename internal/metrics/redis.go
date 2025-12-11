package metrics

import (
	"expvar"
	"strings"
	"sync"
	"sync/atomic"
)

// RedisMetrics captures lightweight Redis gateway stats.
type RedisMetrics struct {
	commandsTotal       atomic.Uint64
	errorsTotal         atomic.Uint64
	connectionsCurrent  atomic.Int64
	connectionsAccepted atomic.Uint64
	mu                  sync.RWMutex
	commandCounts       map[string]*atomic.Uint64
}

func NewRedisMetrics(commandNames []string) *RedisMetrics {
	rm := &RedisMetrics{
		commandCounts: make(map[string]*atomic.Uint64, len(commandNames)),
	}
	for _, name := range commandNames {
		rm.commandCounts[strings.ToUpper(name)] = &atomic.Uint64{}
	}

	expvar.Publish("NoKV.Redis", expvar.Func(func() any {
		return rm.Snapshot()
	}))
	return rm
}

func (rm *RedisMetrics) Snapshot() map[string]any {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	commands := make(map[string]uint64, len(rm.commandCounts))
	for name, counter := range rm.commandCounts {
		commands[name] = counter.Load()
	}

	return map[string]any{
		"commands_total":         rm.commandsTotal.Load(),
		"errors_total":           rm.errorsTotal.Load(),
		"connections_active":     rm.connectionsCurrent.Load(),
		"connections_accepted":   rm.connectionsAccepted.Load(),
		"commands_per_operation": commands,
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
