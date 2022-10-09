package cmd

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/tcw/ibsen/api"
	"github.com/tcw/ibsen/consensus"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	debug                       bool
	trace                       bool
	host                        string
	port                        int
	maxBlockSizeMB              int
	readOnly                    bool
	rootDirectory               string
	benchEntiesByteSize         int
	benchEntiesInEachBatchWrite int
	benchWriteBatches           int
	benchReadBatches            int
	OTELExporterAddr            string
	certKey                     string
	privateKey                  string
	concurrent                  int
	cpuProfile                  string
	memProfile                  string

	rootCmd = &cobra.Command{
		Use:              "ibsen",
		Short:            "Ibsen is a simple log streaming system",
		Long:             `Ibsen is a simple log streaming system build on unix's philosophy of simplicity'`,
		TraverseChildren: true,
	}

	cmdServer = &cobra.Command{
		Use:              "server",
		Short:            "start a ibsen server",
		Long:             `server`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if trace {
				zerolog.SetGlobalLevel(zerolog.TraceLevel)
				log.Info().Msg("logger lever at trace")
			} else if debug {
				zerolog.SetGlobalLevel(zerolog.DebugLevel)
				log.Info().Msg("logger lever at debug")
			} else {
				zerolog.SetGlobalLevel(zerolog.InfoLevel)
				log.Info().Msg("logger lever at info")
			}
			inMemory := false
			var afs *afero.Afero
			absolutePath := "/tmp/data"
			if rootDirectory == "" {
				var fs = afero.NewMemMapFs()
				afs = &afero.Afero{Fs: fs}
				inMemory = true
			} else {
				var err error
				absolutePath, err = filepath.Abs(rootDirectory)
				if err != nil {
					log.Fatal().Err(err)
				}
				exists, err := afs.DirExists(rootDirectory)
				if err != nil {
					log.Fatal().Err(err).Msgf("failed checking if root dir exists")
				}
				if !exists {
					log.Fatal().Msgf("data root path [%s] does not exist", rootDirectory)
				}
				var fs = afero.NewOsFs()
				if readOnly {
					fs = afero.NewReadOnlyFs(fs)
				}
				afs = &afero.Afero{Fs: fs}
			}
			writeLock := absolutePath + string(os.PathSeparator) + ".writeLock"
			lock := consensus.NewFileLock(afs, writeLock, time.Second*10, time.Second*5)
			ibsenServer := api.IbsenServer{
				Readonly:         readOnly,
				Lock:             lock,
				InMemory:         inMemory,
				Afs:              afs,
				RootPath:         absolutePath,
				TTL:              30 * time.Second,
				MaxBlockSize:     maxBlockSizeMB * 1024 * 1024,
				OTELExporterAddr: OTELExporterAddr,
				GRPCCertKey:      AbsOrEmpty(certKey),
				GRPCPrivateKey:   AbsOrEmpty(privateKey),
				CpuProfile:       cpuProfile,
				MemProfile:       memProfile,
			}
			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
			if err != nil {
				log.Err(err)
				return
			}
			err = ibsenServer.Start(lis)
			if err != nil {
				log.Fatal().Err(err)
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
					log.Fatal().Msg(fmt.Sprintf("%s is not a number", batchSizeString))
				}
			}
			file, err := filepath.Abs(args[0])
			if err != nil {
				log.Fatal().Err(err)
			}
			err = ReadLogFile(file, uint32(batchSize))
			if err != nil {
				log.Fatal().Err(err)
			}
		},
	}

	//Todo: fix
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
				log.Fatal().Err(err)
			}
			err = ReadLogIndexFile(absolutePath)
			if err != nil {
				log.Fatal().Err(err)
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
			client, err := newIbsenBench(host + ":" + strconv.Itoa(port))
			if err != nil {
				log.Fatal().Err(err)
			}
			benchmarkReport := ""
			if concurrent > 1 {
				benchmarkReport, err = client.BenchmarkConcurrent(topic, benchEntiesByteSize, benchEntiesInEachBatchWrite, benchWriteBatches, benchReadBatches, concurrent)
				if err != nil {
					log.Fatal().Err(err)
				}
			} else {
				benchmarkReport, err = client.Benchmark(topic, benchEntiesByteSize, benchEntiesInEachBatchWrite, benchWriteBatches, benchReadBatches)
				if err != nil {
					log.Fatal().Err(err)
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
			client, err := newIbsenClient(host + ":" + strconv.Itoa(port))
			if err != nil {
				log.Fatal().Err(err)
			}
			result := ""

			if len(args) > 1 {
				result, err = client.Write(topic, args[1])
				if err != nil {
					log.Fatal().Err(err)
				}
			} else {
				result, err = client.Write(topic)
				if err != nil {
					log.Fatal().Err(err)
				}
			}
			fmt.Println(result)
		},
	}

	cmdClientList = &cobra.Command{
		Use:              "list",
		Short:            "list topics with grpc client",
		Long:             `list topic with grpc client`,
		TraverseChildren: true,
		Args:             cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := newIbsenClient(host + ":" + strconv.Itoa(port))
			if err != nil {
				log.Fatal().Err(err)
			}
			result, err := client.List()
			if err != nil {
				log.Fatal().Err(err)
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
			client, err := newIbsenClient(host + ":" + strconv.Itoa(port))
			if err != nil {
				log.Fatal().Err(err)
			}
			err = client.Read(topic, offset, uint32(batchSize64))
			if err != nil {
				log.Fatal().Err(err)
			}
		},
	}
)

func AbsOrEmpty(path string) string {
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		log.Warn().Msgf("unable to find absolut file path for %s", path)
		return path
	}
	return abs
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	port, _ = strconv.Atoi(getenv("IBSEN_PORT", strconv.Itoa(50001)))
	host = getenv("IBSEN_HOST", "0.0.0.0")
	maxBlockSizeMB, _ = strconv.Atoi(getenv("IBSEN_MAX_BLOCK_SIZE", "1000"))
	readOnly, _ = strconv.ParseBool(getenv("IBSEN_READ_ONLY", "false"))
	rootDirectory = getenv("IBSEN_ROOT_DIRECTORY", "")

	rootCmd.PersistentFlags().IntVarP(&port, "port", "p", port, "config file (default is current directory)")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "l", "0.0.0.0", "config file (default is current directory)")
	rootCmd.PersistentFlags().StringVarP(&certKey, "certKey", "", "", "Certificate key file path for GRPC SLT")
	rootCmd.PersistentFlags().StringVarP(&privateKey, "privateKey", "", "", "Private key file path for GRPC SLT")
	rootCmd.PersistentFlags().StringVarP(&OTELExporterAddr, "OTELExporter", "e", "", "config file (0.0.0.0:4317)")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "v", false, "set logging to debug level")
	rootCmd.PersistentFlags().BoolVarP(&trace, "trace", "t", false, "set logging to trace level")

	cmdServer.Flags().IntVarP(&maxBlockSizeMB, "maxBlockSize", "m", maxBlockSizeMB, "Max MB in log files")
	cmdServer.Flags().BoolVarP(&readOnly, "readOnly", "o", readOnly, "set Ibsen in read only mode")
	cmdServer.Flags().StringVarP(&rootDirectory, "rootDirectory", "d", rootDirectory, "root directory - where ibsen will write all files")
	cmdServer.Flags().StringVarP(&cpuProfile, "cpuProfile", "z", "", "Profile cpu usage")
	cmdServer.Flags().StringVarP(&memProfile, "memProfile", "y", "", "Profile memory usage")

	cmdClientBench.Flags().IntVarP(&benchEntiesByteSize, "byteSize", "", 100, "Entry byte size in bench")
	cmdClientBench.Flags().IntVarP(&benchEntiesInEachBatchWrite, "batchSize", "", 1000, "Entries in each batch in bench")
	cmdClientBench.Flags().IntVarP(&benchWriteBatches, "bwb", "", 1000, "Write in batches of")
	cmdClientBench.Flags().IntVarP(&benchReadBatches, "brb", "", 1000, "Read in batches of")
	cmdClientBench.Flags().IntVarP(&concurrent, "concurrent", "", 1, "Concurrency number")

	//writeEntryByteSize int, writeEntriesInEachBatch int, writeBatches int, readBatchSize int

	rootCmd.AddCommand(cmdServer, cmdClient, cmdTools)
	cmdTools.AddCommand(cmdToolsReadIndexLogFile, cmdToolsReadLogFile)
	cmdClient.AddCommand(cmdClientList, cmdClientWrite, cmdClientRead, cmdClientBench)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
