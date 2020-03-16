package http

import (
	"bufio"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/tcw/ibsen/logStorage"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

type IbsenHttpServer struct {
	Port            uint16
	StorageRootPath string
	MaxBlockSize    int64
	IbsenServer     *http.Server
	Storage         *ext4.LogStorage
}

func NewIbsenHttpServer() *IbsenHttpServer {
	server := IbsenHttpServer{
		Port:            5001,
		StorageRootPath: "",
		MaxBlockSize:    1024 * 1024 * 10,
	}
	storage, err := ext4.NewLogStorage(server.StorageRootPath, server.MaxBlockSize)
	if err != nil {
		log.Fatal("unable to start ibsen server with ext4 storage")
	}
	server.Storage = storage
	return &server
}

func (ibsen *IbsenHttpServer) startHttpServer() {
	var wait time.Duration
	flag.DurationVar(&wait, "graceful-timeout", time.Second*15,
		"the duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m")
	flag.Parse()

	r := mux.NewRouter()
	r.HandleFunc("/write/{topic}", ibsen.writeEntry).Methods("POST")
	r.HandleFunc("/read/{topic}/{offset}", ibsen.readEntry).Methods("GET")
	r.HandleFunc("/create/{topic}", ibsen.createTopic).Methods("GET")
	r.HandleFunc("/drop/{topic}", ibsen.dropTopic).Methods("GET")
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

	//Todo: move to ibsen root package
	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)

	// Block until we receive our signal.
	<-c

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	srv.Shutdown(ctx)
	// Optionally, you could run srv.Shutdown in a goroutine and block on
	// <-ctx.Done() if your application should wait for other services
	// to finalize based on context cancellation.
	log.Println("shutting down")
	os.Exit(0)
}

func sendMessage(logChan chan *logStorage.LogEntry, wg *sync.WaitGroup, w http.ResponseWriter) {
	for {
		entry := <-logChan
		bytes := base64.StdEncoding.EncodeToString(entry.Entry)
		_, err := w.Write([]byte(`{"offset":` + strconv.FormatUint(entry.Offset, 10) + `,"entry": ` + bytes + "}\n"))
		if err != nil {
			return
			w.WriteHeader(http.StatusInternalServerError)
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
