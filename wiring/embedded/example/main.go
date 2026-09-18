// Command example is the smallest useful embedded build: a log in a program, with no server
// around it. It exists to be copied, and to be weighed — scripts/embedded-size.sh builds it
// beside the full server so the difference between the two is a number rather than a claim.
//
// It wires memstore because that keeps the whole program inside the standard library, which
// is what scripts/check-architecture.sh checks. A microcontroller would wire flashstore
// instead and stay just as pure; a program with a filesystem under it would wire aferostore
// and pay for afero.
package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/tcw/ibsen/adapter/driven/blockstore/memstore"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/wiring/embedded"
)

func main() {
	log, err := embedded.Open(embedded.Params{Store: memstore.New()})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer log.Close()

	entries := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	if err = log.Write("events", &entries); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	logChan := make(chan *[]domain.LogEntry)
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		for batch := range logChan {
			for _, entry := range *batch {
				fmt.Printf("%d\t%s\n", entry.Offset, entry.Entry)
			}
			wg.Done()
		}
		close(done)
	}()
	err = log.Read(driver.ReadParams{TopicName: "events", LogChan: logChan, Wg: &wg, BatchSize: 10})
	wg.Wait()
	close(logChan)
	<-done
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
