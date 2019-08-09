package errors

//import (
//	"errors"
//	"fmt"
//	"github.com/pickme-go/k-stream/logger"
//	"runtime"
//)
//
//type Error struct {
//	errors []error
//}
//
//func (e Error) Error() string {
//	str := ""
//	for _, err := range e.errors {
//		str += err.Error() + ", "
//	}
//	return str
//}
//
//func (e Error) Errors() []error {
//	return e.errors
//}
//
//func (e Error) Log(prefix string, message interface{}, previous error, params ...interface{}) {
//	err := newError(prefix, message, previous, params...)
//	if e, ok := err.(Error); ok {
//		for _, er := range e.Errors() {
//			logger.Error(``, er.Error())
//		}
//	}
//}
//
//func New(prefix string, message interface{}, previous error, params ...interface{}) error {
//	return newError(prefix, message, previous, params...)
//}
//
//func newError(prefix string, message interface{}, previous error, params ...interface{}) error {
//
//	current := errors.New(
//		fmt.Sprintf(`[%s] [%+v%s] %v`, prefix, message, filePath(), params))
//
//	if previous == nil {
//		return Error{
//			errors: []error{current},
//		}
//	}
//
//	if previous != nil {
//		e, ok := previous.(Error)
//		if !ok {
//			return Error{
//				errors: []error{previous, current},
//			}
//		}
//
//		e.errors = append(e.errors, Error{
//			errors: []error{current},
//		})
//
//		return e
//	}
//
//	return nil
//}
//
//func filePath() string {
//	_, f, l, ok := runtime.Caller(3)
//	if !ok {
//		f = `<Unknown>`
//		l = 1
//	}
//
//	return fmt.Sprintf(` on %s:%d`, f, l)
//}
