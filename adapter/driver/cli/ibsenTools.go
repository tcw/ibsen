package cli

import (
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/index"
	"github.com/tcw/ibsen/core/logfmt"
	"github.com/tcw/ibsen/core/port/driven"
)

// ReadLogFile prints a log block file, entry by entry. codecs is what its frames are
// resolved against: a block written with the default codec cannot be read without one, and
// this package may not build codecs of its own — a driving adapter does not reach a driven
// adapter, so the composition root hands them over (wiring.ReadCodecs).
func ReadLogFile(fileName string, batchSize uint32, codecs driven.Codecs) error {
	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	terminate := make(chan bool)
	go sendBatchMessage(logChan, &wg, terminate)
	// a log block named on the command line is read as a plain file, outside any store
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = logfmt.ReadFile(logfmt.ReadFileParams{
		Reader:    file,
		Codecs:    codecs,
		LogChan:   logChan,
		Wg:        &wg,
		BatchSize: batchSize,
		EndOffset: math.MaxUint64,
	})
	if err != nil {
		return err
	}
	wg.Wait()
	terminate <- true
	return nil
}

func ReadLogIndexFile(fileName string) error {
	file, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatal().Err(err).Str("file", fileName).Msg("reading file failed")
	}
	log.Info().Str("file", fileName).Msg("read index file")
	idx := index.NewIndex(file)
	if err != nil {
		log.Fatal().Err(err).Str("file", fileName).Msg("marshalling file failed")
	}
	fmt.Println(idx.ToString())
	return nil
}

func sendBatchMessage(logChan chan *[]domain.LogEntry, wg *sync.WaitGroup, terminate chan bool) {
	for {
		select {
		case <-terminate:
			close(logChan)
			return
		case entryBatch := <-logChan:
			batch := *entryBatch
			for _, entry := range batch {
				fmt.Printf("%d\t%s\n", entry.Offset, string(entry.Entry))
			}
			wg.Done()
		}
	}
}
