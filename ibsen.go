package main

import (
	"context"
	"flag"
	"github.com/google/uuid"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"github.com/tcw/ibsen/api/httpApi"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var (
	ibsenId        = uuid.New().String()
	storagePath    = flag.String("s", "", "Where to store logs")
	useHttp        = flag.Bool("h", false, "Use http 1.1 instead of gRPC")
	grpcPort       = flag.Int("p", 50001, "grpc port (default 50001)")
	httpPort       = flag.Int("hp", 5001, "httpApi port (default 5001)")
	maxBlockSizeMB = flag.Int("b", 100, "Max size for each log block in MB")
	cpuprofile     = flag.String("cpu", "", "write cpu profile to `file`")
	memprofile     = flag.String("mem", "", "write memory profile to `file`")
	ibsenFiglet    = `
                           _____ _                    
                          |_   _| |                   
                            | | | |__  ___  ___ _ __  
                            | | | '_ \/ __|/ _ \ '_ \ 
                           _| |_| |_) \__ \  __/ | | |
                          |_____|_.__/|___/\___|_| |_|

	'One should not read to devour, but to see what can be applied.'
	 Henrik Ibsen

`
)

var ibsenGrpcServer *grpcApi.IbsenGrpcServer
var httpServer *http.Server
var writeLock string
var done chan bool = make(chan bool)
var doneCleanup chan bool = make(chan bool)
var locked chan bool = make(chan bool)

func main() {

	flag.Parse()

	go initSignals()

	if *storagePath == "" {
		log.Fatal("Storage path is mandatory (use: -s <path>)")
	}

	abs, err2 := filepath.Abs(*storagePath)
	if err2 != nil {
		log.Fatal(err2)
	}
	storagePath = &abs
	writeLock = abs + string(os.PathSeparator) + ".writeLock"
	log.Printf("Waiting for exclusive write lock on file [%s]...\n", writeLock)
	go acquireLock(writeLock, done, doneCleanup, locked)

	<-locked

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
	}

	storage, err := ext4.NewLogStorage(*storagePath, int64(*maxBlockSizeMB)*1024*1024)
	if err != nil {
		log.Println(err)
		return
	}

	if *useHttp {
		ibsenHttpServer := httpApi.NewIbsenHttpServer(storage)
		ibsenHttpServer.Port = uint16(*httpPort)
		log.Printf("Ibsen http/1.1 server started on port [%d]\n", ibsenHttpServer.Port)
		log.Print(ibsenFiglet)
		httpServer = ibsenHttpServer.StartHttpServer()
	} else {
		ibsenGrpcServer = grpcApi.NewIbsenGrpcServer(storage)
		ibsenGrpcServer.Port = uint16(*grpcPort)
		log.Printf("Ibsen grpc server started on port [%d]\n", ibsenGrpcServer.Port)
		log.Print(ibsenFiglet)
		var err2 error
		err2 = ibsenGrpcServer.StartGRPC()
		if err2 != nil {
			log.Fatal(err2)
		}
	}
	<-doneCleanup
}

func acquireLock(lockFile string, done chan bool, doneCleanUp chan bool, locked chan bool) {
	for {
		file, err := os.OpenFile(lockFile,
			os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)

		if err != nil {
			time.Sleep(time.Second * 3)
			continue
		}
		_, err = file.Write([]byte(ibsenId))
		if err != nil {
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

func initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGABRT)
	signalHandler(<-captureSignal)
}

func shutdownCleanly() {
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	if *useHttp {
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

func signalHandler(signal os.Signal) {
	log.Printf("\nCaught signal: %+v", signal)
	log.Println("\nWait for Ibsen to shutdown...")

	switch signal {

	case syscall.SIGHUP:
		shutdownCleanly()

	case syscall.SIGINT:
		shutdownCleanly()

	case syscall.SIGTERM:
		shutdownCleanly()

	case syscall.SIGQUIT:
		shutdownCleanly()

	case syscall.SIGABRT:
		shutdownCleanly()

	default:
		log.Printf("- unhandled system signal sent to Ibsen [%s], starting none graceful shutdown", signal.String())
		syscall.Exit(0)
	}
}
