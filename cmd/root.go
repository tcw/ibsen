package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/tcw/ibsen/client"
	"github.com/tcw/ibsen/server"
	"github.com/tcw/ibsen/tools"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	storagePath    string
	useHttp        bool
	host           string
	serverPort     int
	maxBlockSizeMB int
	cpuprofile     string
	memprofile     string

	rootCmd = &cobra.Command{
		Use:   "ibsen",
		Short: "Ibsen is a kurbernetes friendly streaming platform",
		Long: `Ibsen builds on the shoulders of giants. Taking advantage of the recent advances in 
			distributed block storage and unix's philosophy of simplicity'`,
	}

	cmdServer = &cobra.Command{
		Use:   "server",
		Short: "server",
		Long:  `server`,
	}

	cmdServerStart = &cobra.Command{
		Use:   "start [data directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			absolutePath, err2 := filepath.Abs(args[0])
			if err2 != nil {
				log.Fatal(err2)
			}
			ibsenServer := server.IbsenServer{
				DataPath:     absolutePath,
				UseHttp:      false,
				Host:         "localhost",
				Port:         50001,
				MaxBlockSize: 100,
				CpuProfile:   "",
				MemProfile:   "",
			}
			ibsenServer.Start()
		},
	}

	cmdClient = &cobra.Command{
		Use:   "client [from directory]",
		Short: "client",
		Long:  `client`,
	}

	cmdClientCreate = &cobra.Command{
		Use:   "create [topic]",
		Short: "create",
		Long:  `create`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.CreateTopic(args[0])
		},
	}

	cmdClientRead = &cobra.Command{
		Use:   "read [topic]",
		Short: "read",
		Long:  `read`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.ReadTopic(args[0])
		},
	}

	cmdClientWrite = &cobra.Command{
		Use:   "write [topic]",
		Short: "write",
		Long:  `write`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.WriteTopic(args[0])
		},
	}

	cmdClientBench = &cobra.Command{
		Use:   "bench",
		Short: "bench",
		Long:  `benchmark`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdCat = &cobra.Command{
		Use:   "cat [from file/directory]",
		Short: "cat",
		Long:  `cat`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			tools.ReadTopic(args[0])
		},
	}

	cmdCheck = &cobra.Command{
		Use:   "check [from directory]",
		Short: "check",
		Long:  `check`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}
)

func startClient() client.IbsenClient {
	serverHost, err := rootCmd.Flags().GetString("host")
	if err != nil {
		log.Fatal(err)
	}
	serverPort, err := rootCmd.Flags().GetInt("port")
	if err != nil {
		log.Fatal(err)
	}
	return client.Start(serverHost + ":" + strconv.Itoa(serverPort))
}

func Execute() {

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	rootCmd.PersistentFlags().BoolVarP(&useHttp, "http1.1", "u", false, "config file (default is current directory)")
	rootCmd.PersistentFlags().IntVarP(&serverPort, "port", "p", 50001, "config file (default is current directory)")
	rootCmd.PersistentFlags().StringVarP(&host, "host", "l", "localhost", "config file (default is current directory)")
	cmdServer.PersistentFlags().IntVarP(&maxBlockSizeMB, "maxBlockSize", "b", 100, "config file (default is current directory)")
	rootCmd.PersistentFlags().StringVarP(&cpuprofile, "cpuprofile", "c", "", "config file (default is current directory)")
	rootCmd.PersistentFlags().StringVarP(&memprofile, "memprofile", "m", "", "config file (default is current directory)")
	err := rootCmd.PersistentFlags().MarkHidden("cpuprofile")
	if err != nil {
		log.Fatal(err)
	}
	err = rootCmd.PersistentFlags().MarkHidden("memprofile")
	if err != nil {
		log.Fatal(err)
	}

	if useHttp && serverPort == 50001 {
		serverPort = 5001
	}

	rootCmd.AddCommand(cmdServer, cmdClient, cmdCat, cmdCheck)
	cmdServer.AddCommand(cmdServerStart)
	cmdClient.AddCommand(cmdClientCreate, cmdClientRead, cmdClientWrite, cmdClientBench)
}
