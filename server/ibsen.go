package server

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"github.com/tcw/ibsen/api/httpApi"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var ibsenGrpcServer *grpcApi.IbsenGrpcServer
var httpServer *http.Server
var writeLock string
var done = make(chan bool)
var doneCleanup = make(chan bool)
var locked = make(chan bool)
var ibsenId = uuid.New().String()

var ibsenFiglet = `
                           _____ _                    
                          |_   _| |                   
                            | | | |__  ___  ___ _ __  
                            | | | '_ \/ __|/ _ \ '_ \ 
                           _| |_| |_) \__ \  __/ | | |
                          |_____|_.__/|___/\___|_| |_|

	'One should not read to devour, but to see what can be applied.'
	 Henrik Ibsen (1828–1906)

`

type IbsenServer struct {
	DataPath     string
	UseHttp      bool
	Host         string
	Port         int
	MaxBlockSize int
	CpuProfile   string
	MemProfile   string
}

func (ibs *IbsenServer) Start() {
	go ibs.initSignals()
	waitForWriteLock(ibs.DataPath)

	useCpuProfiling(ibs.CpuProfile)

	storage, err := ext4.NewLogStorage(ibs.DataPath, int64(ibs.MaxBlockSize)*1024*1024)
	if err != nil {
		log.Println(err)
		return
	}
	if ibs.UseHttp {
		ibs.startHTTPServer(storage)
	} else {
		ibs.startGRPCServer(storage)
	}
	<-doneCleanup
}

func waitForWriteLock(dataPath string) {
	writeLock = dataPath + string(os.PathSeparator) + ".writeLock"
	log.Printf("Waiting for exclusive write lock on file [%s]...\n", writeLock)
	go acquireLock(writeLock, done, doneCleanup, locked)
	<-locked
}

func useCpuProfiling(cpuProfile string) {
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
	}
}

func (ibs *IbsenServer) startHTTPServer(storage ext4.LogStorage) {
	ibsenHttpServer := httpApi.NewIbsenHttpServer(storage)
	ibsenHttpServer.Port = uint16(ibs.Port)
	log.Printf("Ibsen http/1.1 server started on port [%d]\n", ibsenHttpServer.Port)
	fmt.Print(ibsenFiglet)
	httpServer = ibsenHttpServer.StartHttpServer()
}

func (ibs *IbsenServer) startGRPCServer(storage ext4.LogStorage) {
	ibsenGrpcServer = grpcApi.NewIbsenGrpcServer(storage)
	ibsenGrpcServer.Port = uint16(ibs.Port)
	log.Printf("Ibsen grpc server started on port [%d]\n", ibsenGrpcServer.Port)
	fmt.Print(ibsenFiglet)
	var err2 error
	err2 = ibsenGrpcServer.StartGRPC()
	if err2 != nil {
		log.Fatal(err2)
	}
}

func acquireLock(lockFile string, done chan bool, doneCleanUp chan bool, locked chan bool) {
	for {
		file, err := os.OpenFile(lockFile,
			os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)

		if err != nil {
			log.Printf("Unable to acquire lock due to error: %s", err.Error())
			time.Sleep(time.Second * 3)
			continue
		}
		_, err = file.Write([]byte(ibsenId))
		if err != nil {
			log.Printf("Unable to acquire lock due to error: %s", err.Error())
			time.Sleep(time.Second * 3)
			continue
		}
		log.Printf("Got file lock with id [%s]\n", ibsenId)
		locked <- true
		<-done
		err = file.Close()
		if err != nil {
			log.Println(err)
		}
		err = os.Remove(lockFile)
		if err != nil {
			log.Println(err)
		}
		log.Printf("Removed file lock with id [%s]\n", ibsenId)
		doneCleanUp <- true
		break
	}
}

func (ibs *IbsenServer) initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGABRT)
	ibs.signalHandler(<-captureSignal)
}

func (ibs *IbsenServer) ShutdownCleanly() {
	if ibs.CpuProfile != "" {
		pprof.StopCPUProfile()
	}

	if ibs.MemProfile != "" {
		f, err := os.Create(ibs.MemProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	if ibs.UseHttp {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()
		err := httpServer.Shutdown(ctx)
		if err != nil {
			log.Println(err)
		}
	} else {
		ibsenGrpcServer.IbsenServer.GracefulStop()
		log.Println("shutdown gRPC server")
	}
	done <- true

}

func (ibs *IbsenServer) signalHandler(signal os.Signal) {
	log.Printf("\nCaught signal: %+v", signal)
	log.Println("\nWait for Ibsen to shutdown...")

	switch signal {

	case syscall.SIGHUP:
		ibs.ShutdownCleanly()

	case syscall.SIGINT:
		ibs.ShutdownCleanly()

	case syscall.SIGTERM:
		ibs.ShutdownCleanly()

	case syscall.SIGQUIT:
		ibs.ShutdownCleanly()

	case syscall.SIGABRT:
		ibs.ShutdownCleanly()

	default:
		log.Printf("Unexpected system signal [%s] sent to Ibsen. Trying to gracefully shutdown, without any garanties...", signal.String())
		ibs.ShutdownCleanly()
	}
}
