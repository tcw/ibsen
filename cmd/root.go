package cmd

import (
	"fmt"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/tcw/ibsen/api"
	"github.com/tcw/ibsen/client"
	"github.com/tcw/ibsen/consensus"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	inMemory       bool
	host           string
	serverPort     int
	maxBlockSizeMB int
	toBase64       bool
	entryByteSize  int
	entries        int
	batches        int
	cpuProfile     string
	memProfile     string

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

			var afs *afero.Afero
			absolutePath := "/tmp/data"
			if inMemory {
				var fs = afero.NewMemMapFs()
				afs = &afero.Afero{Fs: fs}
			} else {
				var err error
				if len(args) != 1 {
					fmt.Println("No file path to ibsen storage was given, use -i to run in-memory or specify path")
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
			lock := consensus.NewFileLock(afs, writeLock, time.Second*20)
			ibsenServer := api.IbsenServer{
				Lock:         lock,
				InMemory:     inMemory,
				Afs:          afs,
				DataPath:     absolutePath,
				Host:         host,
				Port:         serverPort,
				MaxBlockSize: maxBlockSizeMB,
				CpuProfile:   cpuProfile,
				MemProfile:   memProfile,
			}
			ibsenServer.Start()
		},
	}

	cmdClient = &cobra.Command{
		Use:              "client",
		Short:            "client from commandline",
		Long:             `client`,
		TraverseChildren: true,
	}

	cmdClientCreate = &cobra.Command{
		Use:              "create [topic]",
		Short:            "create a new topic",
		Long:             `create`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.CreateTopic(args[0])
			fmt.Printf("created topic %s\n", args[0])
		},
	}

	cmdClientRead = &cobra.Command{
		Use:              "read [topic] <offset>",
		Short:            "read entries from a topic",
		Long:             `read`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			if len(args) > 1 {
				offset, err := strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					fmt.Printf("Illegal offset [%s]", args[1])
				}
				ibsenClient.ReadTopic(args[0], offset, uint32(entries))
			} else {
				ibsenClient.ReadTopic(args[0], 0, uint32(entries))
			}
		},
	}

	cmdClientWrite = &cobra.Command{
		Use:              "write [topic]",
		Short:            "write entries to a topic",
		Long:             `write`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.WriteTopic(args[0])
		},
	}

	cmdClientStatus = &cobra.Command{
		Use:              "status",
		Short:            "topic status as json",
		Long:             `status`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.Status()
		},
	}

	cmdBench = &cobra.Command{
		Use:              "bench",
		Short:            "benchmark read/write",
		Long:             `bench`,
		TraverseChildren: true,
	}

	cmdBenchWrite = &cobra.Command{
		Use:              "write [topic]",
		Short:            "benchmark write to topic",
		Long:             `bench write`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.BenchWrite(args[0], entryByteSize, entries, batches)
		},
	}

	cmdBenchRead = &cobra.Command{
		Use:              "read [topic]",
		Short:            "benchmark read from topic",
		Long:             `bench read`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.BenchRead(args[0], 0, uint32(entries))
		},
	}

	cmdFile = &cobra.Command{
		Use:              "file",
		Short:            "access files directly from file",
		Long:             `file`,
		TraverseChildren: true,
	}

	cmdCat = &cobra.Command{
		Use:              "cat [from file/directory]",
		Short:            "cat a log file or topic directory",
		Long:             `cat`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var fs = afero.NewOsFs()
			afs := &afero.Afero{Fs: fs}
			client.ReadTopic(afs, args[0], toBase64)
		},
	}

	cmdCheck = &cobra.Command{
		Use:              "check [from directory]",
		Short:            "check for file entry corruption",
		Long:             `check`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}
)

func startClient() client.IbsenClient {
	target := host + ":" + strconv.Itoa(serverPort)
	return client.Start(target)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	serverPort, _ = strconv.Atoi(getenv("IBSEN_PORT", strconv.Itoa(50001)))
	host = getenv("IBSEN_HOST", "0.0.0.0")
	maxBlockSizeMB, _ = strconv.Atoi(getenv("IBSEN_MAX_BLOCK_SIZE", "100"))
	entries, _ = strconv.Atoi(getenv("IBSEN_ENTRIES", "1000"))
	rootCmd.Flags().IntVarP(&serverPort, "port", "p", serverPort, "config file (default is current directory)")
	rootCmd.Flags().StringVarP(&host, "host", "l", "0.0.0.0", "config file (default is current directory)")
	cmdServer.Flags().IntVarP(&maxBlockSizeMB, "maxBlockSize", "m", maxBlockSizeMB, "Max MB in log files")
	cmdServer.Flags().StringVarP(&cpuProfile, "cpuProfile", "z", "", "config file")
	cmdServer.Flags().StringVarP(&memProfile, "memProfile", "y", "", "config file")
	cmdServer.Flags().BoolVarP(&inMemory, "inMemory", "i", false, "run in-memory only")
	cmdCat.Flags().BoolVarP(&toBase64, "base64", "b", false, "Convert messages to base64")
	cmdClient.Flags().IntVarP(&entries, "entries", "e", entries, "Number of entries in each batch")
	cmdBenchWrite.Flags().IntVarP(&entryByteSize, "entryByteSize", "s", 100, "Test data entry size in bytes")
	cmdBenchWrite.Flags().IntVarP(&batches, "batches", "a", 1, "Number of batches")

	rootCmd.AddCommand(cmdServer, cmdClient, cmdFile)
	cmdFile.AddCommand(cmdCat, cmdCheck)
	cmdBench.AddCommand(cmdBenchWrite, cmdBenchRead)
	cmdClient.AddCommand(cmdClientCreate, cmdClientRead, cmdClientWrite, cmdBench, cmdClientStatus)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
