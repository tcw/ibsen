package wiring

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/adapter/driven/blockstore/aferostore"
	"github.com/tcw/ibsen/adapter/driven/locking"
	"github.com/tcw/ibsen/adapter/driven/logging/zerologger"
	"github.com/tcw/ibsen/adapter/driven/telemetry"
	"github.com/tcw/ibsen/adapter/driver/grpcapi"
	"github.com/tcw/ibsen/core/manager"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/errore"
)

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
	Readonly bool
	// Lock is optional: Start builds a file lease over RootPath when none is injected.
	Lock         driven.SingleIbsenWriterLock
	InMemory     bool
	Afs          *afero.Afero
	TTL          time.Duration
	RootPath     string
	MaxBlockSize int
	// IndexSparsity is the number of entries between two index pairs; zero means
	// topic.DefaultIndexSparsity.
	IndexSparsity uint32
	// Codec compresses the frames written from now on, and Codecs resolves the codec byte of
	// frames already written. Both are optional injection points, and what a build puts here
	// is what decides how much compression code it links: leaving them out wires the identity
	// codec and links none. Whatever writes must also be readable, so defaults() folds Codec
	// into Codecs.
	Codec  driven.Codec
	Codecs driven.Codecs
	// FlushEntries and FlushInterval are the durability policy: how many entries may wait
	// for a flush, and how long a batch may be held back hoping for more. Zero entries means
	// topic.DefaultFlushEntries, which makes every write durable before it is acknowledged.
	FlushEntries     uint32
	FlushInterval    time.Duration
	OTELExporterAddr string
	GRPCPrivateKey   string
	GRPCCertKey      string
	CpuProfile       string
	MemProfile       string
	cpuProfileFile   *os.File
	// mu guards topicsManager, grpcServer and the lifecycle channels, which Start sets while
	// a shutdown on another goroutine may read them
	mu            sync.Mutex
	topicsManager *manager.LogTopicsManager
	grpcServer    *grpcapi.IbsenGrpcServer
	shutdownOnce  sync.Once
	stopping      chan struct{} // closed when a shutdown begins
	stopped       chan struct{} // closed when the shutdown has finished
}

// ErrWriteLockUnavailable is returned by Start when another instance holds the single-writer
// lease on the data directory. Start refuses rather than exiting the process, so a program
// that embeds the log can decide what to do; the CLI reports it and exits.
var ErrWriteLockUnavailable = errors.New("single writer lock is held by another instance")

// The single-writer lease lives in the data directory. These are the values the server has
// always used; they belong here rather than in the CLI, because picking the adapter behind a
// port is the composition root's job.
const (
	writeLockFileName = ".writeLock"
	writeLockLease    = 10 * time.Second
	writeLockReclaim  = 5 * time.Second
)

// lifecycle returns the channels tracking this server's shutdown, creating them on first
// use. Start and ShutdownCleanly are called from different goroutines and either can be
// first, so the pair is made once under the mutex rather than assigned by whichever runs
// first. shutdownOnce keeps them from being closed twice.
func (ibs *IbsenServer) lifecycle() (stopping, stopped chan struct{}) {
	ibs.mu.Lock()
	defer ibs.mu.Unlock()
	if ibs.stopping == nil {
		ibs.stopping = make(chan struct{})
		ibs.stopped = make(chan struct{})
	}
	return ibs.stopping, ibs.stopped
}

// defaults fills in the adapters the caller did not inject. It runs before Start begins
// anything that reads them, so the signal handler never races the assignment. A test injects
// its own Lock and keeps it.
func (ibs *IbsenServer) defaults() {
	if ibs.Lock == nil {
		ibs.Lock = locking.NewFileLock(ibs.Afs,
			filepath.Join(ibs.RootPath, writeLockFileName), writeLockLease, writeLockReclaim)
	}
	// a server must be able to read back what it writes, so the write codec is always in the
	// read registry whether or not the caller remembered to put it there
	if ibs.Codec != nil {
		if ibs.Codecs == nil {
			ibs.Codecs = driven.NewCodecs(ibs.Codec)
		} else if _, taken := ibs.Codecs[ibs.Codec.ID()]; !taken {
			ibs.Codecs[ibs.Codec.ID()] = ibs.Codec
		}
	}
}

func (ibs *IbsenServer) Start(listener net.Listener) error {
	ibs.defaults()
	stopping, stopped := ibs.lifecycle()
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
			return errore.WrapWithContextF(ErrWriteLockUnavailable,
				"unable to acquire the single writer lock on path [%s], aborting start", ibs.RootPath)
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
		IndexSparsity:    ibs.IndexSparsity,
		Codec:            ibs.Codec,
		Codecs:           ibs.Codecs,
		FlushEntries:     ibs.FlushEntries,
		FlushInterval:    ibs.FlushInterval,
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
	case <-stopping:
		<-stopped
	default:
	}
	if err != nil {
		return errore.Wrap(err)
	}
	return nil
}

func (ibs *IbsenServer) startGRPCServer(lis net.Listener, manager driver.LogManager) error {
	var server *grpcapi.IbsenGrpcServer
	if ibs.GRPCPrivateKey == "" && ibs.GRPCCertKey == "" {
		log.Warn().Msg("ibsen server is starting in UNSECURE mode")
		server = grpcapi.NewUnsecureIbsenGrpcServer(manager, ibs.TTL, time.Second*2)
	} else {
		server = grpcapi.NewSecureIbsenGrpcServer(manager, grpcapi.GRPCSecurity{
			CertKeyFile:   ibs.GRPCCertKey,
			PrivteKeyFile: ibs.GRPCPrivateKey,
		}, ibs.TTL, time.Second*2)
	}
	ibs.mu.Lock()
	ibs.grpcServer = server
	ibs.mu.Unlock()
	log.Info().Msg(fmt.Sprintf("Started ibsen server on: [%s]", lis.Addr().String()))
	fmt.Print(ibsenFiglet)
	// The exporter keeps its provider alive until the server stops serving, which is what
	// serving signals. Starting it is the composition root's job: the gRPC adapter should
	// not know that telemetry exists.
	var serving sync.WaitGroup
	serving.Add(1)
	if ibs.OTELExporterAddr != "" {
		go telemetry.ConnectToOTELExporter(&serving, ibs.OTELExporterAddr)
	}
	err := server.StartGRPC(lis)
	serving.Done()
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
	stopping, stopped := ibs.lifecycle()
	close(stopping)
	defer close(stopped)

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

	ibs.mu.Lock()
	server := ibs.grpcServer
	ibs.mu.Unlock()

	if server != nil {
		log.Info().Msg("gracefully stopping grpc server...")

		grpcStopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(grpcStopped)
		}()

		t := time.NewTimer(5 * time.Second)
		select {
		case <-t.C:
			log.Info().Msg("stopped gRPC server forcefully")
			server.Stop()
		case <-grpcStopped:
			t.Stop()
		}
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

	if !ibs.InMemory && ibs.Lock != nil {
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
