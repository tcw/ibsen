package main

import (
	"context"
	"flag"
	"fmt"
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

var (
	storagePath    = flag.String("s", "", "Where to store logs")
	grpcPort       = flag.Int("gp", 50001, "grpc port (default 50001)")
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

func main() {

	flag.Parse()

	go initSignals()

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

	ibsenHttpServer := httpApi.NewIbsenHttpServer(storage)
	ibsenHttpServer.Port = uint16(*httpPort)

	httpServer = ibsenHttpServer.StartHttpServer()

	ibsenGrpcServer = grpcApi.NewIbsenGrpcServer(storage)
	ibsenGrpcServer.Port = uint16(*grpcPort)

	fmt.Print(ibsenFiglet)
	fmt.Printf("Ibsen grpc server started on port %d\n", ibsenGrpcServer.Port)
	fmt.Printf("Ibsen http/1.1 server started on port %d\n", ibsenHttpServer.Port)
	var err2 error
	err2 = ibsenGrpcServer.StartGRPC()
	if err != nil { //Todo: fix
		log.Fatal(err2)
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

	fmt.Println("\nIbsen finished server cleanup")
	ibsenGrpcServer.Storage.Close()
	ibsenGrpcServer.IbsenServer.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	httpServer.Shutdown(ctx)

	log.Println("shutting down")
	os.Exit(0)
}

func signalHandler(signal os.Signal) {
	fmt.Printf("\nCaught signal: %+v", signal)
	fmt.Println("\nWait Ibsen to shutdown...")

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
		fmt.Println("- unknown system signal sent to Ibsen")
	}

	os.Exit(0)
}
