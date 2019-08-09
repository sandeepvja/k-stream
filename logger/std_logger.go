package logger

//import (
//	"context"
//	"github.com/Shopify/sarama"
//)
//import (
//	"fmt"
//	"github.com/pickme-go/log"
//)
//
//var Log sarama.StdLogger = &stdLogger{}
//
//type stdLogger struct {
//	logger log.PrefixedLogger
//}
//
//func NewStdLogger() Logger {
//	return &stdLogger{
//		logger: log.Constructor.PrefixedLog(log.FileDepth(3)),
//	}
//}
//
//func (l *stdLogger) Fatal(prefix string, message interface{}, params ...interface{}) {
//	l.logger.Fatal(prefix, message, params)
//}
//
//func (l *stdLogger) Error(prefix string, message interface{}, params ...interface{}) {
//	l.logger.DefaultLogger.Error(prefix, message, params)
//}
//
//func (l *stdLogger) Warn(prefix string, message interface{}, params ...interface{}) {
//	l.logger.Warn(prefix, message, params)
//}
//
//func (l *stdLogger) Debug(prefix string, message interface{}, params ...interface{}) {
//	l.logger.Debug(prefix, message, params)
//}
//
//func (l *stdLogger) Info(prefix string, message interface{}, params ...interface{}) {
//	l.logger.Info(prefix, message, params)
//}
//
//func (l *stdLogger) Trace(prefix string, message interface{}, params ...interface{}) {
//	l.logger.Trace(prefix, message, params)
//}
//
//func (l *stdLogger) FatalContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
//	l.logger.FatalContext(ctx, prefix, message, params...)
//}
//func (l *stdLogger) ErrorContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
//	l.logger.ErrorContext(ctx, prefix, message, params...)
//}
//func (l *stdLogger) WarnContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
//	l.logger.WarnContext(ctx, prefix, message, params...)
//}
//func (l *stdLogger) DebugContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
//	l.logger.DebugContext(ctx, prefix, message, params...)
//}
//func (l *stdLogger) InfoContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
//	l.logger.InfoContext(ctx, prefix, message, params...)
//}
//func (l *stdLogger) TraceContext(ctx context.Context, prefix string, message interface{}, params ...interface{}) {
//	l.logger.TraceContext(ctx, prefix, message, params...)
//}
//
//func (l *stdLogger) Print(v ...interface{}) {
//	l.logger.Debug(`k-stream.logger`, fmt.Sprintf(`%+v`, v...))
//}
//
//func (l *stdLogger) Printf(format string, v ...interface{}) {
//	l.logger.Debug(`k-stream.logger`, fmt.Sprintf(format, v...))
//}
//
//func (l *stdLogger) Println(v ...interface{}) {
//	l.logger.Debug(`k-stream.logger`, fmt.Sprintf(`%+v`, v...))
//}
