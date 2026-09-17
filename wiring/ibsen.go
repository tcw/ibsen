package wiring

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driven/blockstore/aferostore"
	"github.com/tcw/ibsen/adapter/driven/logging/zerologger"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/errore"
)

var ibsenGrpcServer *grpcapi.IbsenGrpcServer
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
	Readonly         bool
	Lock             driven.SingleIbsenWriterLock
	InMemory         bool
	Afs              *afero.Afero
	TTL              time.Duration
	RootPath         string
	MaxBlockSize     int
	OTELExporterAddr string
	GRPCPrivateKey   string
	GRPCCertKey      string
	CpuProfile       string
	MemProfile       string
	cpuProfileFile   *os.File
	// mu guards topicsManager, which Start sets while the signal handler may read it
	mu            sync.Mutex
	topicsManager *manager.LogTopicsManager
	shutdownOnce  sync.Once
	stopping      chan struct{} // closed when a shutdown begins
	stopped       chan struct{} // closed when the shutdown has finished
}

func (ibs *IbsenServer) Start(listener net.Listener) error {
	ibs.stopping = make(chan struct{})
	ibs.stopped = make(chan struct{})
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
			return errore.Wrap(err)
		}
		if !exists {
			return errore.NewF("path [%s] does not exist, will not start unless existing path is specified", ibs.RootPath)
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
			return errore.Wrap(err)
		}
		if err := pprof.StartCPUProfile(ibs.cpuProfileFile); err != nil {
			return errore.WrapError(ibs.cpuProfileFile.Close(), err)
		}
		log.Info().Msg(fmt.Sprintf("Started profiling, creating file %s", ibs.CpuProfile))
	}

	topicsManager, err := manager.NewLogTopicsManager(manager.LogTopicManagerParams{
		ReadOnly:         ibs.Readonly,
		Store:            aferostore.New(ibs.Afs, ibs.RootPath),
		TTL:              ibs.TTL,
		CheckForNewEvery: time.Second * 2,
		MaxBlockSize:     ibs.MaxBlockSize,
		Logger:           zerologger.New(log.Logger),
	})
	if err != nil {
		return errore.Wrap(err)
	}
	ibs.mu.Lock()
	ibs.topicsManager = &topicsManager
	ibs.mu.Unlock()
	err = ibs.startGRPCServer(listener, &topicsManager)
	// gRPC stops serving early in a shutdown, and the process exits once Start returns, so
	// wait until the shutdown has closed the log and released the lock
	select {
	case <-ibs.stopping:
		<-ibs.stopped
	default:
	}
	if err != nil {
		return errore.Wrap(err)
	}
	return nil
}

func (ibs *IbsenServer) startGRPCServer(lis net.Listener, manager driver.LogManager) error {
	if ibs.GRPCPrivateKey == "" && ibs.GRPCCertKey == "" {
		log.Warn().Msg("ibsen server is starting in UNSECURE mode")
		ibsenGrpcServer = grpcapi.NewUnsecureIbsenGrpcServer(manager, ibs.TTL, time.Second*2)
	} else {
		ibsenGrpcServer = grpcapi.NewSecureIbsenGrpcServer(manager, grpcapi.GRPCSecurity{
			CertKeyFile:   ibs.GRPCCertKey,
			PrivteKeyFile: ibs.GRPCPrivateKey,
		}, ibs.TTL, time.Second*2)
	}
	log.Info().Msg(fmt.Sprintf("Started ibsen server on: [%s]", lis.Addr().String()))
	fmt.Print(ibsenFiglet)
	var wg sync.WaitGroup
	wg.Add(1)
	err := ibsenGrpcServer.StartGRPC(lis, &wg, ibs.OTELExporterAddr)
	wg.Done()
	if err != nil {
		return errore.Wrap(err)
	}
	return nil
}

func (ibs *IbsenServer) initSignals() {
	var captureSignal = make(chan os.Signal, 1)
	signal.Notify(captureSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGABRT)
	ibs.signalHandler(<-captureSignal)
}

// ShutdownCleanly stops the gRPC server, waits for in-flight writes and background indexing
// to finish, and then releases the single-writer lock. Later calls wait for the first.
func (ibs *IbsenServer) ShutdownCleanly() {
	ibs.shutdownOnce.Do(ibs.shutdown)
}

func (ibs *IbsenServer) shutdown() {
	if ibs.stopping != nil {
		close(ibs.stopping)
		defer close(ibs.stopped)
	}

	// profiling failures are logged but do not stop the shutdown, which still has to release the lock
	if ibs.MemProfile != "" {
		if err := writeHeapProfile(ibs.MemProfile); err != nil {
			log.Error().Err(err).Msgf("unable to write memory profile %s", ibs.MemProfile)
		} else {
			log.Info().Msg(fmt.Sprintf("Ended memory profiling, writing to file %s", ibs.MemProfile))
		}
	}

	if ibs.CpuProfile != "" {
		log.Info().Msg(fmt.Sprintf("Ended cpu profiling, writing to file %s", ibs.CpuProfile))
		pprof.StopCPUProfile()
		err := ibs.cpuProfileFile.Close()
		if err != nil {
			log.Error().Err(err).Msgf("unable to close cpu profile %s", ibs.CpuProfile)
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

	// gRPC does not wait for the handlers of a forced stop, and writes still index in the
	// background. The lock is only released once nothing writes, even if that takes long:
	// another instance must never write beside this one.
	ibs.mu.Lock()
	topicsManager := ibs.topicsManager
	ibs.mu.Unlock()
	if topicsManager != nil {
		log.Info().Msg("waiting for in-flight writes and indexing to finish...")
		topicsManager.Close()
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

func writeHeapProfile(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		return errore.WrapError(f.Close(), err)
	}
	return f.Close()
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
