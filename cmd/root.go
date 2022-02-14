package cmd

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/tcw/ibsen/api"
	"github.com/tcw/ibsen/consensus"
	"github.com/tcw/ibsen/errore"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	host                   string
	serverPort             int
	maxBlockSizeMB         int
	benchEntiesByteSize    int
	benchEntiesInEachBatch int
	benchWriteBaches       int
	benchReadBatches       int
	concurrent             int
	cpuProfile             string
	memProfile             string

	rootCmd = &cobra.Command{
		Use:              "ibsen",
		Short:            "Ibsen is a simple log streaming system",
		Long:             `Ibsen is a simple log streaming system build on unix's philosophy of simplicity'`,
		TraverseChildren: true,
	}

	cmdServer = &cobra.Command{
		Use:              "server [data directory]",
		Short:            "start a ibsen server",
		Long:             `server`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			inMemory := false
			var afs *afero.Afero
			absolutePath := "/tmp/data"
			if len(args) == 0 {
				var fs = afero.NewMemMapFs()
				afs = &afero.Afero{Fs: fs}
				inMemory = true
			} else {
				var err error
				if len(args) != 1 {
					fmt.Println("No file path to ibsen data directory was given, use -i to run in-memory or specify path")
					return
				}
				absolutePath, err = filepath.Abs(args[0])
				if err != nil {
					log.Fatal(err)
				}
				var fs = afero.NewOsFs()
				afs = &afero.Afero{Fs: fs}
			}
			writeLock := absolutePath + string(os.PathSeparator) + ".writeLock"
			lock := consensus.NewFileLock(afs, writeLock, time.Second*10, time.Second*5)
			ibsenServer := api.IbsenServer{
				Lock:         lock,
				InMemory:     inMemory,
				Afs:          afs,
				RootPath:     absolutePath,
				MaxBlockSize: maxBlockSizeMB,
				CpuProfile:   cpuProfile,
				MemProfile:   memProfile,
			}
			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, serverPort))
			if err != nil {
				log.Println(errore.SprintTrace(err))
				return
			}
			err = ibsenServer.Start(lis)
			if err != nil {
				log.Fatalf(errore.SprintTrace(err))
			}
		},
	}

	cmdClient = &cobra.Command{
		Use:              "client",
		Short:            "access files directly from file",
		Long:             `file`,
		TraverseChildren: true,
	}

	cmdTools = &cobra.Command{
		Use:              "tools",
		Short:            "access files directly from file",
		Long:             `file`,
		TraverseChildren: true,
	}

	cmdToolsReadLogFile = &cobra.Command{
		Use:              "read-log [file] [optional batch size (default 1000)]",
		Short:            "read ibsen log file from disk",
		Long:             `file`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			batchSize := 1000
			var err error
			if len(args) < 1 {
				fmt.Println("First parameter must be file name")
				return
			}
			if len(args) == 2 {
				batchSizeString := args[1]
				batchSize, err = strconv.Atoi(batchSizeString)
				if err != nil {
					log.Fatalf("%s is not a number", batchSizeString)
				}
			}
			file, err := filepath.Abs(args[0])
			if err != nil {
				log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
			}
			err = ReadLogFile(file, uint32(batchSize))
			if err != nil {
				log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
			}
		},
	}

	cmdToolsReadIndexLogFile = &cobra.Command{
		Use:              "read-index [file]",
		Short:            "read ibsen index file from disk",
		Long:             `file`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("First parameter must be file name")
				return
			}
			absolutePath, err := filepath.Abs(args[0])
			if err != nil {
				log.Fatal(err)
			}
			err = ReadLogIndexFile(absolutePath)
			if err != nil {
				log.Fatalln(errore.SprintTrace(errore.WrapWithContext(err)))
			}
		},
	}

	cmdClientBench = &cobra.Command{
		Use:              "bench [options] [topic]",
		Short:            "bench ibsen",
		Long:             `file`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("First parameter must be topic name")
				return
			}
			topic := args[0]
			client := newIbsenBench(host + ":" + strconv.Itoa(serverPort))
			benchmarkReport := ""
			var err error
			if concurrent > 1 {
				benchmarkReport, err = client.BenchmarkConcurrent(topic, benchEntiesByteSize, benchEntiesInEachBatch, benchWriteBaches, benchReadBatches, concurrent)
				if err != nil {
					log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
				}
			} else {
				benchmarkReport, err = client.Benchmark(topic, benchEntiesByteSize, benchEntiesInEachBatch, benchWriteBaches, benchReadBatches)
				if err != nil {
					log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
				}
			}
			fmt.Println(benchmarkReport)
		},
	}

	cmdClientWrite = &cobra.Command{
		Use:              "write",
		Short:            "write with grpc client",
		Long:             `file`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("First parameter must be topic name")
				return
			}
			topic := args[0]
			client := newIbsenClient(host + ":" + strconv.Itoa(serverPort))
			result := ""
			var err error
			if len(args) > 1 {
				result, err = client.Write(topic, args[1])
				if err != nil {
					log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
				}
			} else {
				result, err = client.Write(topic)
				if err != nil {
					log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
				}
			}
			fmt.Println(result)
		},
	}
	cmdClientRead = &cobra.Command{
		Use:              "read [file] [offset (default=0)] [batch size (default=1000)]",
		Short:            "read with grpc client",
		Long:             `file`,
		TraverseChildren: true,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 1 {
				fmt.Println("First parameter must be topic name")
				return
			}
			topic := args[0]
			var offset uint64 = 0
			var batchSize64 uint64 = 1000
			var err error
			if len(args) == 2 {
				offset, err = strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					fmt.Printf("offset %s not a uint64", args[1])
				}
			}
			if len(args) == 3 {
				batchSize64, err = strconv.ParseUint(args[2], 10, 32)
				if err != nil {
					fmt.Printf("offset %s not a uint64", args[1])
				}
			}
			client := newIbsenClient(host + ":" + strconv.Itoa(serverPort))
			err = client.Read(topic, offset, uint32(batchSize64))
			if err != nil {
				log.Fatal(errore.SprintTrace(errore.WrapWithContext(err)))
			}
		},
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	serverPort, _ = strconv.Atoi(getenv("IBSEN_PORT", strconv.Itoa(50001)))
	host = getenv("IBSEN_HOST", "0.0.0.0")
	maxBlockSizeMB, _ = strconv.Atoi(getenv("IBSEN_MAX_BLOCK_SIZE", "1000"))
	rootCmd.Flags().IntVarP(&serverPort, "port", "p", serverPort, "config file (default is current directory)")
	rootCmd.Flags().StringVarP(&host, "host", "l", "0.0.0.0", "config file (default is current directory)")
	cmdServer.Flags().IntVarP(&maxBlockSizeMB, "maxBlockSize", "m", maxBlockSizeMB, "Max MB in log files")

	cmdServer.Flags().StringVarP(&cpuProfile, "cpuProfile", "z", "", "Profile cpu usage")
	cmdServer.Flags().StringVarP(&memProfile, "memProfile", "y", "", "Profile memory usage")

	cmdClientBench.Flags().IntVarP(&benchEntiesByteSize, "bwe", "e", 100, "Entry byte size in bench")
	cmdClientBench.Flags().IntVarP(&benchEntiesInEachBatch, "ben", "b", 1000, "Entries in each batch in bench")
	cmdClientBench.Flags().IntVarP(&benchReadBatches, "brb", "r", 1000, "Read in batches of")
	cmdClientBench.Flags().IntVarP(&benchWriteBaches, "bwb", "w", 1000, "Write in batches of")
	cmdClientBench.Flags().IntVarP(&concurrent, "concurrent", "c", 1, "Concurrency number")

	rootCmd.AddCommand(cmdServer, cmdClient, cmdTools)
	cmdTools.AddCommand(cmdToolsReadIndexLogFile, cmdToolsReadLogFile)
	cmdClient.AddCommand(cmdClientWrite, cmdClientRead, cmdClientBench)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
