package logger

import (
	"context"
	"github.com/pickme-go/log"
)

type TestingLogger interface {
	Errorf(string, ...interface{})
}

var DefaultLogger = log.Constructor.PrefixedLog(log.FileDepth(3), log.WithLevel(log.ERROR), log.WithColors(false))

type Logger interface {
	Fatal(prefix string, message interface{}, params ...interface{})
	Error(prefix string, message interface{}, params ...interface{})
	Warn(prefix string, message interface{}, params ...interface{})
	Debug(prefix string, message interface{}, params ...interface{})
	Info(prefix string, message interface{}, params ...interface{})
	Trace(prefix string, message interface{}, params ...interface{})
	FatalContext(ctx context.Context, prefix string, message interface{}, params ...interface{})
	ErrorContext(ctx context.Context, prefix string, message interface{}, params ...interface{})
	WarnContext(ctx context.Context, prefix string, message interface{}, params ...interface{})
	DebugContext(ctx context.Context, prefix string, message interface{}, params ...interface{})
	InfoContext(ctx context.Context, prefix string, message interface{}, params ...interface{})
	TraceContext(ctx context.Context, prefix string, message interface{}, params ...interface{})
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
