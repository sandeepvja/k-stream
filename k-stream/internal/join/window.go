package join

import "sync"

type Window struct {
	*sync.Mutex
	window map[interface{}]interface{}
}

func NewWindow() *Window {
	return &Window{
		new(sync.Mutex),
		make(map[interface{}]interface{}),
	}
}

func (w *Window) Write(key, value interface{}) {
	w.Lock()
	defer w.Unlock()
	w.window[key] = value
}

func (w *Window) Read(key interface{}) (interface{}, bool) {
	w.Lock()
	defer w.Unlock()

	v, ok := w.window[key]
	return v, ok
}
