package httpApi

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

type IbsenHttpServer struct {
	Port        uint16
	IbsenServer *http.Server
	Storage     *ext4.LogStorage
}

func NewIbsenHttpServer(storage *ext4.LogStorage) *IbsenHttpServer {
	server := IbsenHttpServer{
		Port:    5001,
		Storage: storage,
	}
	return &server
}

func (ibsen *IbsenHttpServer) StartHttpServer() *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/write/{topic}", ibsen.writeEntry).Methods("POST")
	r.HandleFunc("/read/{topic}/{offset}", ibsen.readEntry).Methods("GET")
	r.HandleFunc("/create/{topic}", ibsen.createTopic).Methods("POST")
	r.HandleFunc("/drop/{topic}", ibsen.dropTopic).Methods("POST")
	r.HandleFunc("/list/topic", ibsen.listTopic).Methods("GET")

	srv := &http.Server{
		Addr:         "0.0.0.0:" + strconv.Itoa(int(ibsen.Port)),
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()
	return srv
}

func sendMessage(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, w http.ResponseWriter) {
	for {
		entry := <-logChan
		bytes := base64.StdEncoding.EncodeToString(entry.Entry)
		_, err := w.Write([]byte(`{"offset":` + strconv.FormatUint(entry.Offset, 10) + `,"entry": "` + bytes + `""}\n`))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		wg.Done()
	}
}

func (ibsen *IbsenHttpServer) writeEntry(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	body := r.Body
	defer body.Close()
	scanner := bufio.NewScanner(body)
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	for scanner.Scan() {
		text := scanner.Text()
		log.Println(text)
		bytes, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			fmt.Println(err)
			return
		}
		//Todo: use batch write
		_, err = ibsen.Storage.Write(&logStorage.TopicMessage{
			Topic:   vars["topic"],
			Message: bytes,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (ibsen *IbsenHttpServer) readEntry(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	logChan := make(chan *logStorage.LogEntry)
	var wg sync.WaitGroup
	go sendMessage(logChan, &wg, w)
	offset, err := strconv.ParseUint(vars["offset"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	err = ibsen.Storage.ReadFromNotIncluding(logChan, &wg, vars["topic"], offset)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	wg.Wait()
	w.WriteHeader(http.StatusOK)
}
func (ibsen *IbsenHttpServer) listTopic(w http.ResponseWriter, r *http.Request) {
	topics, err := ibsen.Storage.ListTopics()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = w.Write([]byte("[" + strings.Join(topics, ",") + "]"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
func (ibsen *IbsenHttpServer) createTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_, err := ibsen.Storage.Create(vars["topic"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
func (ibsen *IbsenHttpServer) dropTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_, err := ibsen.Storage.Drop(vars["topic"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
