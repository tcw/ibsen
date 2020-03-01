package main

import (
	"flag"
	"fmt"
	grpcApi "github.com/tcw/ibsen/api/grpc/golangApi"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var (
	startServer = flag.Bool("s", false, "Run server")
	startClient = flag.Bool("c", false, "Run client")
	cpuprofile  = flag.String("cpu", "", "write cpu profile to `file`")
	memprofile  = flag.String("mem", "", "write memory profile to `file`")
)

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

	if *startServer {
		fmt.Println("started server on port 50001")
		err := grpcApi.StartGRPC(50001, "cert.pem", "key.pen", false, "/home/tom/tmp/doskey")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	signalHandler(<-captureSignal)
}

func signalHandler(signal os.Signal) {
	fmt.Printf("\nCaught signal: %+v", signal)
	fmt.Println("\nWait for 1 second to finish processing")
	time.Sleep(1 * time.Second)

	switch signal {

	case syscall.SIGHUP: // kill -SIGHUP XXXX
		fmt.Println("- got hungup")

	case syscall.SIGINT: // kill -SIGINT XXXX or Ctrl+c
		fmt.Println("- got ctrl+c")

	case syscall.SIGTERM: // kill -SIGTERM XXXX
		fmt.Println("- got force stop")

	case syscall.SIGQUIT: // kill -SIGQUIT XXXX
		fmt.Println("- stop and core dump")

	default:
		fmt.Println("- unknown signal")
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

	fmt.Println("\nFinished server cleanup")
	os.Exit(0)
}
