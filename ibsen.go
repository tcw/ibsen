package main

import (
	"flag"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"github.com/tcw/ibsen/logStorage/unix/ext4"
	"google.golang.org/grpc"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

var (
	storagePath = flag.String("s", "", "Where to store logs")
	grpcPort    = flag.Int("p", 50001, "grpc port (default 50001)")
	cpuprofile  = flag.String("cpu", "", "write cpu profile to `file`")
	memprofile  = flag.String("mem", "", "write memory profile to `file`")
)

var ibsenServer *grpc.Server
var storage *ext4.LogStorage

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

	if *storagePath == "" {
		fmt.Println("Storage path is mandatory and must exist(use -s).")
	}

	fmt.Println("started server on port 50001")
	var err error
	ibsenServer, storage, err = grpcApi.StartGRPC(uint16(*grpcPort), "cert.pem", "key.pen", false, *storagePath)
	if err != nil {
		log.Fatal(err)
	}
}

func initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	signalHandler(<-captureSignal)
}

func shutdownCleanly() {
	storage.Close()
	ibsenServer.GracefulStop()
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
	os.Exit(0)
}
