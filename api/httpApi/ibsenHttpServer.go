package httpApi

import (
	"bufio"
	"encoding/base64"
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
		//Todo: use batch write
		_, err := ibsen.Storage.Write(&logStorage.TopicMessage{
			Topic:   vars["topic"],
			Message: []byte(text),
		})
		if err != nil {
			log.Println(err)
		}
	}
}

func (ibsen *IbsenHttpServer) readEntry(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	logChan := make(chan logStorage.LogEntry)
	var wg sync.WaitGroup
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/x-ndjson")
	go sendMessage(logChan, &wg, w)
	offset, err := strconv.ParseUint(vars["offset"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if offset == 0 {
		err = ibsen.Storage.ReadFromBeginning(logChan, &wg, vars["topic"])
	} else {
		err = ibsen.Storage.ReadFromNotIncluding(logChan, &wg, vars["topic"], offset)
	}

	if err != nil {
		log.Println(err)
		return
	}
	wg.Wait()
	w.(http.Flusher).Flush()
}
func (ibsen *IbsenHttpServer) listTopic(w http.ResponseWriter, r *http.Request) {
	topics, err := ibsen.Storage.ListTopics()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = w.Write([]byte("[" + strings.Join(topics, ",") + "]"))
	if err != nil {
		log.Println(err)
	}
}
func (ibsen *IbsenHttpServer) createTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_, err := ibsen.Storage.Create(vars["topic"])
	if err != nil {
		log.Println(err)
		return
	}
}
func (ibsen *IbsenHttpServer) dropTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	w.WriteHeader(http.StatusOK)
	_, err := ibsen.Storage.Drop(vars["topic"])
	if err != nil {
		log.Println(err)
	}
}

func sendMessage(logChan chan logStorage.LogEntry, wg *sync.WaitGroup, w http.ResponseWriter) {
	for {
		entry := <-logChan
		bytes := base64.StdEncoding.EncodeToString(entry.Entry)
		ndjson := `[` + strconv.FormatUint(entry.Offset, 10) + `, "` + bytes + "\"]\n"
		_, err := w.Write([]byte(ndjson))
		if err != nil {
			log.Println(err)
			return
		}
		wg.Done()
	}
}
