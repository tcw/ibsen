package api

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/api/grpcApi"
	"github.com/tcw/ibsen/consensus"
	"github.com/tcw/ibsen/errore"
	"github.com/tcw/ibsen/manager"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var ibsenGrpcServer *grpcApi.IbsenGrpcServer
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
	Readonly       bool
	Lock           consensus.Lock
	InMemory       bool
	Afs            *afero.Afero
	TTL            time.Duration
	RootPath       string
	MaxBlockSize   int
	CpuProfile     string
	MemProfile     string
	cpuProfileFile *os.File
}

func (ibs *IbsenServer) Start(listener net.Listener) error {
	go ibs.initSignals()
	log.Info().Msg(fmt.Sprintf("Using listener: %s", listener.Addr().String()))
	if ibs.Readonly {
		log.Info().Msg("running in read only mode")
	}
	if ibs.InMemory {
		log.Info().Msg("running in-memory only mode")
		err := ibs.Afs.Mkdir(ibs.RootPath, 0600)
		if err != nil {
			return err
		}
	} else {
		exists, err := ibs.Afs.Exists(ibs.RootPath)
		if err != nil {
			return err
		}
		if !exists {
			return errore.NewWithContext("path [%s] does not exist, will not start unless existing path is specified", ibs.RootPath)
		}
		log.Info().Msg(fmt.Sprintf("Waiting for single writer lock on file [%s]...", ibs.RootPath))
		if !ibs.Readonly && !ibs.Lock.AcquireLock() {
			log.Fatal().Msg(fmt.Sprintf("failed trying to acquire single writer lock on path [%s], aborting start!", ibs.RootPath))
		}
	}

	if ibs.CpuProfile != "" {
		var err error
		ibs.cpuProfileFile, err = os.Create(ibs.CpuProfile)
		if err != nil {
			log.Fatal().Err(err)
		}
		if err := pprof.StartCPUProfile(ibs.cpuProfileFile); err != nil {
			log.Fatal().Err(err)
		}
		log.Info().Msg(fmt.Sprintf("Started profiling, creating file %s", ibs.CpuProfile))
	}

	topicsManager, err := manager.NewLogTopicsManager(ibs.Afs, ibs.Readonly, time.Minute*10, time.Second*5, ibs.RootPath, ibs.MaxBlockSize)
	if err != nil {
		return err
	}
	err = ibs.startGRPCServer(listener, &topicsManager)
	if err != nil {
		return err
	}
	return nil
}

func (ibs *IbsenServer) startGRPCServer(lis net.Listener, manager manager.LogManager) error {
	ibsenGrpcServer = grpcApi.NewUnsecureIbsenGrpcServer(manager)
	log.Info().Msg(fmt.Sprintf("Started ibsen server on: [%s]", lis.Addr().String()))
	fmt.Print(ibsenFiglet)
	err := ibsenGrpcServer.StartGRPC(lis)
	if err != nil {
		return err
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
			log.Fatal().Err(err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			ioErr := f.Close()
			if ioErr != nil {
				log.Fatal().Err(errore.WrapWithError(ioErr, err))
			}
			log.Fatal().Err(err)
		}
		log.Info().Msg(fmt.Sprintf("Ended memory profiling, writing to file %s", ibs.MemProfile))
	}

	if ibs.CpuProfile != "" {
		log.Info().Msg(fmt.Sprintf("Ended cpu profiling, writing to file %s", ibs.CpuProfile))
		pprof.StopCPUProfile()
		err := ibs.cpuProfileFile.Close()
		if err != nil {
			log.Fatal().Err(err)
		}
	}

	log.Info().Msg("gracefully stopping grpc server...")

	stopped := make(chan struct{})
	go func() {
		ibsenGrpcServer.IbsenServer.GracefulStop()
		close(stopped)
	}()

	t := time.NewTimer(5 * time.Second)
	select {
	case <-t.C:
		log.Info().Msg("stopped gRPC server forcefully")
		ibsenGrpcServer.IbsenServer.Stop()
	case <-stopped:
		t.Stop()
	}

	if !ibs.InMemory {
		isReleased := ibs.Lock.ReleaseLock()
		if isReleased {
			log.Info().Msg(fmt.Sprintf("single writer lock [%s] was released!\n", ibs.RootPath))
		} else {
			log.Info().Msg(fmt.Sprintf("unable to release single writer lock [%s]\n", ibs.RootPath))
		}
	}
}

func (ibs *IbsenServer) signalHandler(signal os.Signal) {
	log.Info().Msg(fmt.Sprintf("Ibsen server recieved signal: %+v", signal))

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
		log.Info().Msg(fmt.Sprintf("recived system signal [%s]. Starting gracefully shutdown...", signal.String()))
		ibs.ShutdownCleanly()
		break
	default:
		log.Info().Msg(fmt.Sprintf("recived unexpected system signal [%s]. Trying to gracefully shutdown, without any garanties...", signal.String()))
		ibs.ShutdownCleanly()
	}
}
