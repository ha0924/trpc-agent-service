// 设计依据：docs/技术设计方案.md §7.3「Metrics、Trace 和审计」、§7.4「密钥和脱敏」
//                docs/多租户与节点部署设计.md §3「租户隔离」日志隔离与脱敏

// Package log configures structured logging and redacts credentials.
//
// Two properties matter more than convenience here:
//
//   - Every request-scoped line carries the same identity fields, taken from
//     the RequestContext. Gateway and Worker are separate processes, so
//     trace_id is the only thing that makes their logs joinable — inventing
//     per-call-site key names would break that.
//   - Credentials never reach the output. Redaction happens at the logger,
//     not at each call site, because one forgotten call site is enough to
//     leak a token into a log that is retained for months.
package log

import (
	"context"
	"log/slog"
	"os"
	"regexp"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Setup installs the process-wide logger and returns it.
func Setup(level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:       parseLevel(level),
		ReplaceAttr: redactAttr,
	}

	var h slog.Handler
	if strings.EqualFold(format, "json") {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(h)
	slog.SetDefault(logger)
	return logger
}

func parseLevel(s string) slog.Level {
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

// With returns a logger carrying the request's identity fields.
//
// Call this once per request and pass the result down, rather than repeating
// the fields at each call site.
func With(logger *slog.Logger, rc *types.RequestContext) *slog.Logger {
	if rc == nil {
		return logger
	}
	attrs := make([]any, 0, 14)
	for k, v := range rc.LogFields() {
		if v != "" {
			attrs = append(attrs, k, v)
		}
	}
	return logger.With(attrs...)
}

// FromContext returns a logger carrying whatever identity ctx holds.
func FromContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	rc, err := types.FromContext(ctx)
	if err != nil {
		return logger
	}
	return With(logger, rc)
}

// sensitiveKeys are attribute names whose values are replaced wholesale.
var sensitiveKeys = map[string]bool{
	"password":      true,
	"passwd":        true,
	"secret":        true,
	"token":         true,
	"api_key":       true,
	"apikey":        true,
	"authorization": true,
	"cookie":        true,
	"dsn":           true,
	"credential":    true,
	"access_token":  true,
	"secret_ref":    false, // a secret:// reference is safe: it names, not reveals
}

// Redacted replaces a sensitive value in log output.
const Redacted = "[REDACTED]"

// valuePatterns catch credentials embedded inside an otherwise innocuous
// string, such as a DSN in a connection error or a bearer token in a URL.
var valuePatterns = []*regexp.Regexp{
	// user:password@host in a DSN or URL
	regexp.MustCompile(`(?i)([a-z0-9._-]+):([^@\s/]{1,256})@`),
	// Bearer / Basic authorization values
	regexp.MustCompile(`(?i)(bearer|basic)\s+[A-Za-z0-9._\-+/=]{8,}`),
	// Common provider key shapes
	regexp.MustCompile(`(?i)\bsk-[A-Za-z0-9]{8,}`),
	// key=value pairs naming a credential.
	//
	// The value must not start with "/" so that a secret:// reference is left
	// intact: it names a credential without revealing one, and it is the main
	// clue when diagnosing a resolution failure.
	regexp.MustCompile(`(?i)\b(password|passwd|token|api_?key|secret)\s*[=:]\s*[^\s,;&/][^\s,;&]*`),
}

// redactAttr scrubs one attribute before it is written.
func redactAttr(_ []string, a slog.Attr) slog.Attr {
	if redact, known := sensitiveKeys[strings.ToLower(a.Key)]; known {
		if redact {
			return slog.String(a.Key, Redacted)
		}
		return a
	}
	if a.Value.Kind() == slog.KindString {
		if scrubbed := Scrub(a.Value.String()); scrubbed != a.Value.String() {
			return slog.String(a.Key, scrubbed)
		}
	}
	return a
}

// Scrub removes credential-shaped substrings from s.
//
// Exported so error paths can clean a message before it reaches an audit
// record or an HTTP response, not only the log.
func Scrub(s string) string {
	if s == "" {
		return s
	}
	out := s
	for _, re := range valuePatterns {
		out = re.ReplaceAllStringFunc(out, func(match string) string {
			// Keep the leading identifier so the message stays diagnosable —
			// "root:[REDACTED]@tcp(...)" still tells you which user failed.
			if idx := strings.IndexAny(match, ":="); idx > 0 {
				sep := match[idx : idx+1]
				tail := ""
				if strings.HasSuffix(match, "@") {
					tail = "@"
				}
				return match[:idx] + sep + Redacted + tail
			}
			return Redacted
		})
	}
	return out
}
