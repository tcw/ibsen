package api

import (
	"fmt"
	"github.com/spf13/afero"
	grpcApi "github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/consensus"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/storage"
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
var fProfile *os.File

type IbsenServer struct {
	Lock         consensus.Lock
	InMemory     bool
	Afs          *afero.Afero
	DataPath     string
	Host         string
	Port         int
	MaxBlockSize int
	CpuProfile   string
	MemProfile   string
}

func (ibs *IbsenServer) Start() {
	go ibs.initSignals()

	exists, err := ibs.Afs.Exists(ibs.DataPath)
	if err != nil {
		log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
	}
	if !exists {
		log.Fatalf("path [%s] does not exist, will not start unless existing path is specified", ibs.DataPath)
	}

	if ibs.InMemory {
		log.Println("Running in-memory only!")
	} else {
		log.Printf("Waiting for single writer lock on file [%s]...\n", ibs.DataPath)
		if !ibs.Lock.AcquireLock() {
			log.Fatalf("failed trying to acquire single writer lock on path [%s], aborting start!", ibs.DataPath)
		}
	}

	useCpuProfiling(ibs.CpuProfile)

	start := time.Now()
	logStorage, err := storage.NewLogStorage(ibs.Afs, ibs.DataPath, int64(ibs.MaxBlockSize)*1024*1024)
	stop := time.Now()
	log.Printf("loaded existing topic in [%s]", stop.Sub(start).String())
	if err != nil {
		log.Println(errore.SprintTrace(err))
		return
	}

	err = ibs.startGRPCServer(logStorage)
	if err != nil {
		log.Printf(errore.SprintTrace(err))
		ibs.ShutdownCleanly()
	}
}

func useCpuProfiling(cpuProfile string) {
	if cpuProfile != "" {
		var err error
		fProfile, err = os.Create(cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(fProfile); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		log.Printf("Started profiling, creating file %s", cpuProfile)
	}
}

func (ibs *IbsenServer) startGRPCServer(storage storage.LogStorage) error {
	ibsenGrpcServer = grpcApi.NewIbsenGrpcServer(storage)
	ibsenGrpcServer.Port = uint16(ibs.Port)
	ibsenGrpcServer.Host = ibs.Host
	log.Printf("Ibsen grpc server started on [%s:%d]\n", ibsenGrpcServer.Host, ibsenGrpcServer.Port)
	fmt.Print(ibsenFiglet)
	err := ibsenGrpcServer.StartGRPC()
	if err != nil {
		return errore.WrapWithContext(err)
	}
	return nil
}

func (ibs *IbsenServer) initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGABRT)
	ibs.signalHandler(<-captureSignal)
}

func (ibs *IbsenServer) ShutdownCleanly() {

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
		log.Printf("Ended memory profiling, writing to file %s", ibs.MemProfile)
	}

	log.Printf("gracefully stopping grpc server on port [%d]...", ibs.Port)

	stopped := make(chan struct{})
	go func() {
		ibsenGrpcServer.IbsenServer.GracefulStop()
		close(stopped)
	}()

	t := time.NewTimer(5 * time.Second)
	select {
	case <-t.C:
		log.Println("stopped gRPC server forcefully")
		ibsenGrpcServer.IbsenServer.Stop()
	case <-stopped:
		t.Stop()
	}

	if !ibs.InMemory {
		isReleased := ibs.Lock.ReleaseLock()
		if isReleased {
			log.Printf("single writer lock [%s] was released!\n", ibs.DataPath)
		} else {
			log.Printf("unable to release single writer lock [%s]\n", ibs.DataPath)
		}
	}

	if ibs.CpuProfile != "" {
		log.Printf("Ended cpu profiling, writing to file %s", ibs.CpuProfile)
		pprof.StopCPUProfile()
		err := fProfile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (ibs *IbsenServer) signalHandler(signal os.Signal) {
	log.Printf("\nIbsen server recieved signal: %+v", signal)

	switch signal {
	case syscall.SIGHUP:
		fallthrough
	case syscall.SIGINT:
		fallthrough
	case syscall.SIGTERM:
		fallthrough
	case syscall.SIGQUIT:
		fallthrough
	case syscall.SIGABRT:
		log.Printf("recived system signal [%s]. Starting gracefully shutdown...", signal.String())
		ibs.ShutdownCleanly()
		break
	default:
		log.Printf("recived unexpected system signal [%s]. Trying to gracefully shutdown, without any garanties...", signal.String())
		ibs.ShutdownCleanly()
	}
}
