package obs

import (
	"log/slog"
	"os"
	"strings"
)

// Structured logging (G5).
//
// The point is CORRELATION, not tidiness. A sharded multi-process cluster
// produces interleaved output from many Raft groups across many processes; a line
// that does not carry (node, shard, term, index) cannot be placed in the sequence
// of events it belongs to. Debugging the 2PC recovery race in G3 meant reading
// exactly that kind of interleaving, by hand, from lines that had to be
// reconstructed.
//
// log/slog is stdlib, so this costs no dependency — the same constraint that
// chose net/rpc over gRPC and hand-written Prometheus text over the client
// library.

// LogFormat selects the output encoding.
type LogFormat string

const (
	// LogText is human-readable key=value, for a terminal.
	LogText LogFormat = "text"

	// LogJSON is one JSON object per line, for a log aggregator.
	LogJSON LogFormat = "json"
)

// NewLogger builds a structured logger tagged with this node's identity.
//
// Every line carries the node id, so output from several processes interleaved
// into one stream stays attributable. Per-shard loggers add the shard id via
// ForShard.
func NewLogger(nodeID string, format LogFormat, level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	switch format {
	case LogJSON:
		h = slog.NewJSONHandler(os.Stderr, opts)
	default:
		h = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(h).With("node", nodeID)
}

// ForShard returns a logger that tags every line with a shard id.
//
// Without this, a process hosting several Raft groups produces lines that are
// individually sensible and collectively unreadable: "became leader (term 3)"
// says nothing about WHICH group, and a sharded node has several answering at once.
func ForShard(l *slog.Logger, shardID string) *slog.Logger {
	return l.With("shard", shardID)
}

// ParseLogFormat converts a flag value, defaulting to text.
func ParseLogFormat(s string) LogFormat {
	if strings.EqualFold(s, string(LogJSON)) {
		return LogJSON
	}
	return LogText
}

// ParseLogLevel converts a flag value, defaulting to info.
//
// An unrecognised value falls back rather than failing startup: a mistyped log
// level must never be the reason a bank node refuses to boot.
func ParseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
