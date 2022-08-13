package test

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/errore"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	var fs = afero.NewMemMapFs()
	//var fs = afero.NewOsFs()
	afs = &afero.Afero{Fs: fs}
	//file, err := startCpuPprof()
	//assert.Nil(t, err)
	go startGrpcServer(afs, "/tmp/data")
	//go startGrpcServer(afs, "/home/tom/Ibsen/data")

	params := SimulationParams{
		topics:       50,
		users:        100,
		dataLimit:    10 * 1024 * 1024,
		testDuration: time.Second * 10,
		writeDelay: RandomizedTimeInterval{
			min: time.Millisecond * 100,
			max: time.Millisecond * 500,
		},
		entries: RandomizedSizeInterval{
			min: 1,
			max: 1000,
		},
	}

	simulation, err := newSimulation(params)
	assert.Nil(t, err)
	simulation.start(t)
	//stopCpuPprof(err, file)
	//memProfile()
}

func stopCpuPprof(err error, file *os.File) {
	pprof.StopCPUProfile()
	err = file.Close()
	if err != nil {
		log.Fatal().Err(err)
	}
}

func startCpuPprof() (*os.File, error) {
	file, err := os.Create("cpu.pprof")
	if err != nil {
		log.Fatal().Err(err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatal().Err(err)
	}
	return file, err
}

func memProfile() {
	f, err := os.Create("mem.pprof")
	if err != nil {
		log.Fatal().Err(err)
	}
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		ioErr := f.Close()
		if ioErr != nil {
			log.Fatal().Err(errore.WrapError(ioErr, err))
		}
		log.Fatal().Err(err)
	}
	log.Info().Msg(fmt.Sprintf("Ended memory profiling, writing to file mem.pprof"))
}
