package main

import (
	"context"
	"flag"
	"fmt"
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
	maxBlockSizeMB = flag.Int("b", 100, "Max size for each log block")
	cpuprofile     = flag.String("cpu", "", "write cpu profile to `file`")
	memprofile     = flag.String("mem", "", "write memory profile to `file`")
	ibsenFiglet    = `

                           _____ _                    
                          |_   _| |                   
                            | | | |__  ___  ___ _ __  
                            | | | '_ \/ __|/ _ \ '_ \ 
                           _| |_| |_) \__ \  __/ | | |
                          |_____|_.__/|___/\___|_| |_|

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
	fmt.Printf("Waiting to acquire lock on file [%s]\n", writeLock)
	go acuqireLock(writeLock, done, doneCleanup, locked)

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
		fmt.Printf("Ibsen http/1.1 server started on port %d\n", ibsenHttpServer.Port)
		fmt.Print(ibsenFiglet)
		httpServer = ibsenHttpServer.StartHttpServer()
	} else {
		ibsenGrpcServer = grpcApi.NewIbsenGrpcServer(storage)
		ibsenGrpcServer.Port = uint16(*grpcPort)
		fmt.Printf("Ibsen grpc server started on port %d\n", ibsenGrpcServer.Port)
		fmt.Print(ibsenFiglet)
		var err2 error
		err2 = ibsenGrpcServer.StartGRPC()
		if err2 != nil {
			log.Fatal(err2)
		}
	}
	<-doneCleanup
}

func acuqireLock(lockFile string, done chan bool, doneCleanUp chan bool, locked chan bool) {
	for {
		file, err := os.OpenFile(lockFile,
			os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)

		if err != nil {
			log.Println("Unable to open file", err)
			time.Sleep(time.Second * 3)
			continue
		}
		_, err = file.Write([]byte(ibsenId))
		if err != nil {
			log.Println("unable to write ibsen id to file", err)
			time.Sleep(time.Second * 3)
			continue
		}
		fmt.Printf("Got file lock with ibsen id [%s]\n", ibsenId)
		locked <- true
		b := <-done
		if b {
			fmt.Println("starting file lock removal")
		}
		err = file.Close()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("closed lock file")
		err = os.Remove(lockFile)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Removed file lock with ibsen id [%s]\n", ibsenId)
		doneCleanUp <- true
		break
	}
}

func initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
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
			fmt.Println(err)
		}
	} else {
		ibsenGrpcServer.IbsenServer.GracefulStop()
		fmt.Println("shutdown gRPC server")
	}
	done <- true

}

func signalHandler(signal os.Signal) {
	fmt.Printf("\nCaught signal: %+v", signal)
	fmt.Println("\nWait for Ibsen to shutdown...")

	switch signal {

	case syscall.SIGHUP: // kill -SIGHUP XXXX
		shutdownCleanly()

	case syscall.SIGINT: // kill -SIGINT XXXX or Ctrl+c
		shutdownCleanly()

	case syscall.SIGTERM: // kill -SIGTERM XXXX
		shutdownCleanly()

	case syscall.SIGQUIT: // kill -SIGQUIT XXXX
		shutdownCleanly()

	default:
		fmt.Printf("- unhandled system signal sent to Ibsen [%s], starting none graseful shutdown", signal.String())
	}
}
