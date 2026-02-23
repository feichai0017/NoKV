package NoKV

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	toml "github.com/pelletier/go-toml/v2"
)

// LoadOptionsFile loads engine Options from a TOML file. Unspecified
// fields keep their defaults from NewDefaultOptions.
func LoadOptionsFile(path string) (*Options, error) {
	opt := NewDefaultOptions()
	if err := ApplyOptionsFile(opt, path); err != nil {
		return nil, err
	}
	return opt, nil
}

// ApplyOptionsFile overlays TOML options on top of an existing Options struct.
func ApplyOptionsFile(opt *Options, path string) error {
	if opt == nil {
		return fmt.Errorf("options is nil")
	}
	if path == "" {
		return fmt.Errorf("options file path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".toml" && ext != ".tml" {
		return fmt.Errorf("unsupported options format %q (use .toml)", ext)
	}

	var raw map[string]any
	if err := toml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode toml: %w", err)
	}

	return applyOptionsMap(opt, raw)
}

func applyOptionsMap(opt *Options, raw map[string]any) error {
	if raw == nil {
		return nil
	}
	v := reflect.ValueOf(opt).Elem()
	t := v.Type()
	fields := make(map[string]int, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		fields[normalizeOptionKey(f.Name)] = i
	}

	for key, val := range raw {
		idx, ok := fields[normalizeOptionKey(key)]
		if !ok {
			return fmt.Errorf("unknown option %q", key)
		}
		field := v.Field(idx)
		if !field.CanSet() {
			continue
		}
		converted, err := convertOptionValue(val, field.Type())
		if err != nil {
			return fmt.Errorf("option %q: %w", key, err)
		}
		field.Set(converted)
	}
	return nil
}

func normalizeOptionKey(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, ".", "")
	return s
}

var durationType = reflect.TypeFor[time.Duration]()

func convertOptionValue(val any, typ reflect.Type) (reflect.Value, error) {
	if typ == durationType {
		d, err := parseDurationValue(val)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(d).Convert(typ), nil
	}

	switch typ.Kind() {
	case reflect.Bool:
		b, ok := val.(bool)
		if !ok {
			if s, ok := val.(string); ok {
				parsed, err := strconv.ParseBool(s)
				if err != nil {
					return reflect.Value{}, err
				}
				b = parsed
			} else {
				return reflect.Value{}, fmt.Errorf("expected bool, got %T", val)
			}
		}
		return reflect.ValueOf(b).Convert(typ), nil
	case reflect.String:
		s, ok := val.(string)
		if !ok {
			return reflect.Value{}, fmt.Errorf("expected string, got %T", val)
		}
		return reflect.ValueOf(s).Convert(typ), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := toInt64(val)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(typ).Elem()
		out.SetInt(n)
		return out, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := toUint64(val)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(typ).Elem()
		out.SetUint(n)
		return out, nil
	case reflect.Float32, reflect.Float64:
		n, err := toFloat64(val)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(typ).Elem()
		out.SetFloat(n)
		return out, nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported type %s", typ.String())
	}
}

func parseDurationValue(val any) (time.Duration, error) {
	switch v := val.(type) {
	case string:
		return time.ParseDuration(v)
	case float64:
		return time.Duration(int64(v)), nil
	case float32:
		return time.Duration(int64(v)), nil
	case int:
		return time.Duration(v), nil
	case int64:
		return time.Duration(v), nil
	case int32:
		return time.Duration(v), nil
	case uint64:
		return time.Duration(v), nil
	case uint32:
		return time.Duration(v), nil
	case nil:
		return 0, fmt.Errorf("duration is null")
	default:
		return 0, fmt.Errorf("expected duration string or number, got %T", val)
	}
}

func toInt64(val any) (int64, error) {
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("expected integer, got %T", val)
	}
}

func toUint64(val any) (uint64, error) {
	switch v := val.(type) {
	case float64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case uint32:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("expected unsigned integer, got %T", val)
	}
}

func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("expected float, got %T", val)
	}
}
