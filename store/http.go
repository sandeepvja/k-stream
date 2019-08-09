package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pickme-go/k-stream/logger"
	"net/http"
	"strconv"
)

type Err struct {
	Err string `json:"error"`
}

func MakeEndpoints(host string, registry Registry) {

	r := mux.NewRouter()

	r.HandleFunc(`/stores`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		err := encodeStores(writer, registry.List())
		if err != nil {
			encodeError(err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/stores/{store}`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		vars := mux.Vars(request)
		store, ok := vars[`store`]
		if !ok {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, `unknown route parameter`)
			return
		}

		if !storeExist(store, registry) {
			res := encodeError(errors.New(`store dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
				return
			}
		}

		i, err := registry.Store(store).GetAll(context.Background())
		if err != nil {
			res := encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
				return
			}
		}

		err = encodeAll(writer, i)
		if err != nil {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/stores/{store}/{key}`, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")

		vars := mux.Vars(request)
		store, ok := vars[`store`]
		if !ok {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, `unknown route parameter`)
			return
		}

		if !storeExist(store, registry) {
			res := encodeError(errors.New(`store dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
				return
			}
		}

		key, ok := vars[`key`]
		if !ok {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, `unknown route parameter`)
			return
		}

		//@FIXME
		keyInt, err := strconv.Atoi(key)
		if err != nil {
			return
		}

		data, err := registry.Store(store).Get(context.Background(), keyInt)
		if err != nil {
			res := encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
				return
			}
		}

		err = encodeItem(writer, key, data)
		if err != nil {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
		}

	}).Methods(http.MethodGet)

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(host), handlers.CORS()(r))
		if err != nil {
			logger.DefaultLogger.Error(`k-stream.Store.Http`,
				fmt.Sprintf(`Cannot start web server : %+v`, err))
		}
	}()

	logger.DefaultLogger.Info(`k-stream.Store.Http`, fmt.Sprintf(`Http server started on %s`, host))

}

func encodeStores(w http.ResponseWriter, data []string) error {
	return json.NewEncoder(w).Encode(data)
}

func encodeAll(w http.ResponseWriter, i Iterator) error {

	var keyVals []struct {
		Key   interface{} `json:"key"`
		Value interface{} `json:"value"`
	}

	for i.Valid() {

		keyVal := struct {
			Key   interface{} `json:"key"`
			Value interface{} `json:"value"`
		}{}

		k, err := i.Key()
		if err != nil {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
			i.Next()
			continue
		}

		v, err := i.Value()
		if err != nil {
			logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
			i.Next()
			continue
		}

		keyVal.Key = k
		keyVal.Value = v

		keyVals = append(keyVals, keyVal)
		i.Next()
	}

	return json.NewEncoder(w).Encode(keyVals)
}

func encodeItem(w http.ResponseWriter, k interface{}, v interface{}) error {

	keyVal := struct {
		Key   interface{} `json:"key"`
		Value interface{} `json:"value"`
	}{}

	keyVal.Key = k
	keyVal.Value = v

	return json.NewEncoder(w).Encode(keyVal)
}

func encodeError(e error) []byte {
	byt, err := json.Marshal(Err{
		Err: e.Error(),
	})
	if err != nil {
		logger.DefaultLogger.Error(`k-stream.Store.Http`, err)
	}

	return byt
}

func storeExist(store string, registry Registry) bool {
	for _, s := range registry.List() {
		if s == store {
			return true
		}
	}

	return false
}
