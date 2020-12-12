package httpApi

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"github.com/tcw/ibsen/logStorage"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

type IbsenHttpServer struct {
	Port        uint16
	IbsenServer *http.Server
	Storage     logStorage.LogStorage
}

func NewIbsenHttpServer(storage logStorage.LogStorage) *IbsenHttpServer {
	server := IbsenHttpServer{
		Port:    5001,
		Storage: storage,
	}
	return &server
}

func (ibsen *IbsenHttpServer) StartHttpServer() *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/write/{topic}", ibsen.writeEntry).Methods("POST")
	r.HandleFunc("/read/{topic}/{offset}", ibsen.readEntry).Methods("GET").Queries("base64", "{base64}")
	r.HandleFunc("/read/{topic}", ibsen.readEntry).Methods("GET").Queries("base64", "{base64}")
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
	const maxCapacity = 1024 * 1024 * 1024 * 10 //max 10 MB pr line
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	var bytes = make([][]byte, 0)
	line := 0
	for scanner.Scan() {
		text := scanner.Text()
		tmp := make([]byte, 0)
		tmp = append(tmp, []byte(text)...)
		bytes = append(bytes, tmp)
		line = line + 1
	}

	_, err := ibsen.Storage.WriteBatch(&logStorage.TopicBatchMessage{
		Topic:   vars["topic"],
		Message: bytes,
	})
	if err != nil {
		w.WriteHeader(500)
	}
}

func (ibsen *IbsenHttpServer) readEntry(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	logChan := make(chan *logStorage.LogEntryBatch)
	var wg sync.WaitGroup
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/x-ndjson")
	var err error

	var b64 = false
	if value := r.URL.Query().Get("base64"); value != "" {
		b64, err = strconv.ParseBool(value)
	}

	go sendMessage(logChan, &wg, w, b64)

	var offset uint64 = 0
	if offsetText, ok := vars["offset"]; ok {
		offset, err = strconv.ParseUint(offsetText, 10, 64)
	}

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ibsen.Storage.ReadBatchFromOffsetNotIncluding(logStorage.ReadBatchParam{
		LogChan:   logChan,
		Wg:        &wg,
		Topic:     vars["topic"],
		BatchSize: 1000,
		Offset:    offset,
	})

	if err != nil {
		log.Println(err)
		return
	}
	wg.Wait()
	w.(http.Flusher).Flush()
}
func (ibsen *IbsenHttpServer) listTopic(w http.ResponseWriter, r *http.Request) {
	status := ibsen.Storage.Status()
	prettyJSON, err := json.MarshalIndent(status, "", "    ")
	if err != nil {
		log.Print(err)
	}
	_, err = w.Write(prettyJSON)
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

func sendMessage(logChan chan *logStorage.LogEntryBatch, wg *sync.WaitGroup, w http.ResponseWriter, b64 bool) {
	for {
		entry := <-logChan
		entries := entry.Entries
		for _, logEntry := range entries {
			bytes := string(logEntry.Entry)
			if b64 {
				bytes = base64.StdEncoding.EncodeToString(logEntry.Entry)
			}
			ndjson := `[` + strconv.FormatUint(logEntry.Offset, 10) + `, "` + bytes + "\"]\n"
			_, err := w.Write([]byte(ndjson))
			if err != nil {
				log.Println(err)
				return
			}
		}
		wg.Done()
	}
}
