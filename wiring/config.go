package wiring

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/topic"
)

// ConfigFileName is the per-directory settings file, read from the data directory unless
// --config points elsewhere. Settings kept beside the data travel with it, and the server and
// ibsen append read the same one.
const ConfigFileName = "ibsen.json"

// ErrInvalidConfig is returned for a settings file that cannot be used: unreadable, not JSON,
// an unknown key, or a value that does not parse. It is refused at startup rather than
// half-applied, since a retention limit quietly ignored deletes nothing and one quietly
// misread deletes too much.
var ErrInvalidConfig = errors.New("invalid configuration file")

// Config is the settings file. Today it holds per-topic retention; it is shaped so that later
// per-topic settings, such as compression or the flush mode, sit beside it in the same entry.
//
//	{"topics": {"audit": {"retainAge": "720h", "retainBytes": "50GB"}}}
type Config struct {
	Topics map[string]TopicConfig `json:"topics"`
}

// TopicConfig is one topic's settings. A topic with an entry gets exactly what the entry
// says: its retention replaces the global limits as a whole, so an audit topic given only an
// age keeps everything younger than it, however large. "0" or an absent field is no limit.
type TopicConfig struct {
	RetainAge   string `json:"retainAge,omitempty"`
	RetainBytes string `json:"retainBytes,omitempty"`
}

// Retention is what a data directory keeps: limits for every topic, and overrides.
type Retention struct {
	Default   topic.RetentionPolicy
	Overrides map[domain.TopicName]topic.RetentionPolicy
}

// For is the policy one topic gets.
func (r Retention) For(name domain.TopicName) topic.RetentionPolicy {
	if override, ok := r.Overrides[name]; ok {
		return override
	}
	return r.Default
}

// Keeps reports whether nothing is ever deleted.
func (r Retention) Keeps() bool {
	if !r.Default.Keeps() {
		return false
	}
	for _, policy := range r.Overrides {
		if !policy.Keeps() {
			return false
		}
	}
	return true
}

// LoadRetention builds what a data directory keeps from the global limits and the settings
// file. path is the file --config named; empty means <root>/ibsen.json, which may be absent.
// A file that was named must exist. The file is only ever read.
func LoadRetention(root, path, retainAge, retainBytes string) (Retention, error) {
	retention, err := GlobalRetention(retainAge, retainBytes)
	if err != nil {
		return Retention{}, err
	}
	named := path != ""
	if !named {
		path = filepath.Join(root, ConfigFileName)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && !named {
			return retention, nil
		}
		return Retention{}, fmt.Errorf("%w: %s: %v", ErrInvalidConfig, path, err)
	}
	config, err := ParseConfig(content)
	if err != nil {
		return Retention{}, fmt.Errorf("%w: %s: %v", ErrInvalidConfig, path, err)
	}
	retention.Overrides = map[domain.TopicName]topic.RetentionPolicy{}
	for name, settings := range config.Topics {
		if err := domain.ValidateTopicName(domain.TopicName(name)); err != nil {
			return Retention{}, fmt.Errorf("%w: %s: topic %q: %v", ErrInvalidConfig, path, name, err)
		}
		policy, err := parseRetention(settings.RetainAge, settings.RetainBytes)
		if err != nil {
			return Retention{}, fmt.Errorf("%w: %s: topic %q: %v", ErrInvalidConfig, path, name, err)
		}
		retention.Overrides[domain.TopicName(name)] = policy
	}
	return retention, nil
}

// GlobalRetention is the limits every topic gets, from the flags alone.
func GlobalRetention(retainAge, retainBytes string) (Retention, error) {
	policy, err := parseRetention(retainAge, retainBytes)
	if err != nil {
		return Retention{}, fmt.Errorf("%w: the global retention: %v", ErrInvalidConfig, err)
	}
	return Retention{Default: policy}, nil
}

// ParseConfig reads a settings file, refusing unknown keys and anything after the object.
func ParseConfig(content []byte) (Config, error) {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, err
	}
	if decoder.More() {
		return Config{}, errors.New("unexpected content after the settings object")
	}
	return config, nil
}

func parseRetention(age, size string) (topic.RetentionPolicy, error) {
	var policy topic.RetentionPolicy
	var err error
	if policy.MaxAge, err = ParseAge(age); err != nil {
		return policy, err
	}
	if policy.MaxBytes, err = ParseBytes(size); err != nil {
		return policy, err
	}
	return policy, nil
}

// ParseAge reads a retention age: a Go duration such as 168h or 90m, or a whole number of
// days such as 7d. Empty or 0 is no limit.
func ParseAge(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" {
		return 0, nil
	}
	if days, ok := strings.CutSuffix(value, "d"); ok {
		n, err := strconv.ParseUint(days, 10, 32)
		if err != nil || n == 0 {
			return 0, fmt.Errorf("retainAge %q is not a duration such as 168h or 7d", value)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	age, err := time.ParseDuration(value)
	if err != nil || age < 0 {
		return 0, fmt.Errorf("retainAge %q is not a duration such as 168h or 7d", value)
	}
	return age, nil
}

var byteUnits = []struct {
	suffix string
	factor int64
}{
	// longest first, so KiB is not read as K and a stray iB
	{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
	{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000}, {"TB", 1000 * 1000 * 1000 * 1000},
	{"K", 1000}, {"M", 1000 * 1000}, {"G", 1000 * 1000 * 1000}, {"T", 1000 * 1000 * 1000 * 1000},
	{"B", 1},
}

// ParseBytes reads a retention size: a number of bytes, or one with a unit — KB, MB, GB, TB in
// powers of 1000, KiB, MiB, GiB, TiB in powers of 1024. Empty or 0 is no limit.
func ParseBytes(value string) (int64, error) {
	trimmed := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(value), " ", ""))
	if trimmed == "" || trimmed == "0" {
		return 0, nil
	}
	factor := int64(1)
	for _, unit := range byteUnits {
		if number, ok := strings.CutSuffix(trimmed, unit.suffix); ok {
			trimmed, factor = number, unit.factor
			break
		}
	}
	n, err := strconv.ParseFloat(trimmed, 64)
	if err != nil || n <= 0 || n*float64(factor) > float64(1<<62) {
		return 0, fmt.Errorf("retainBytes %q is not a size such as 10GB or 512MiB", value)
	}
	return int64(n * float64(factor)), nil
}

// DefaultRetentionInterval is how often a server applies retention.
const DefaultRetentionInterval = time.Minute

// retainer is what the retention ticker needs of a log.
type retainer interface {
	Retain(policy func(domain.TopicName) topic.RetentionPolicy, now time.Time) error
}

// startRetention applies retention every interval until the returned stop is called, which
// waits for a run under way. It is the server's, like the flush ticker: the core reads no
// clock, so the time each run is given is read here.
func startRetention(log retainer, retention Retention, interval time.Duration) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				if err := log.Retain(retention.For, now); err != nil {
					logRetentionFailure(err)
				}
			}
		}
	}()
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		ticker.Stop()
		close(done)
		<-finished
	}
}

func logRetentionFailure(err error) {
	zlog.Warn().Err(err).Msg("retention could not remove everything it meant to; it tries again next time")
}
