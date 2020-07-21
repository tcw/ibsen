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
	useHttp        bool
	host           string
	serverPort     int
	maxBlockSizeMB int
	toBase64       bool
	cpuprofile     string
	memprofile     string

	rootCmd = &cobra.Command{
		Use:   "ibsen",
		Short: "Ibsen is a simple log streaming system",
		Long: `Ibsen builds on the shoulders of giants. Taking advantage of the recent advances in 
			distributed block storage and unix's philosophy of simplicity'`,
		TraverseChildren: true,
	}

	cmdServer = &cobra.Command{
		Use:              "server [data directory]",
		Short:            "server",
		Long:             `server`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			absolutePath, err2 := filepath.Abs(args[0])
			if err2 != nil {
				log.Fatal(err2)
			}
			ibsenServer := server.IbsenServer{
				DataPath:     absolutePath,
				UseHttp:      useHttp,
				Host:         host,
				Port:         serverPort,
				MaxBlockSize: maxBlockSizeMB,
				CpuProfile:   "",
				MemProfile:   "",
			}
			ibsenServer.Start()
		},
	}

	cmdClient = &cobra.Command{
		Use:              "client",
		Short:            "client",
		Long:             `client`,
		TraverseChildren: true,
	}

	cmdClientCreate = &cobra.Command{
		Use:              "create [topic]",
		Short:            "create",
		Long:             `create`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("creating topic %s", args[0])
			ibsenClient := startClient()
			ibsenClient.CreateTopic(args[0])
		},
	}

	cmdClientRead = &cobra.Command{
		Use:              "read [topic] <offset>",
		Short:            "read",
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
				ibsenClient.ReadTopicFromNotIncludingOffset(args[0], offset)
			} else {
				ibsenClient.ReadTopic(args[0])
			}
		},
	}

	cmdClientWrite = &cobra.Command{
		Use:              "write [topic]",
		Short:            "write",
		Long:             `write`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ibsenClient := startClient()
			ibsenClient.WriteTopic(args[0])
		},
	}

	cmdClientBench = &cobra.Command{
		Use:              "bench",
		Short:            "bench",
		Long:             `benchmark`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdFile = &cobra.Command{
		Use:              "files",
		Short:            "files",
		Long:             `files`,
		TraverseChildren: true,
	}

	cmdCat = &cobra.Command{
		Use:              "cat [from file/directory]",
		Short:            "cat",
		Long:             `cat`,
		TraverseChildren: true,
		Args:             cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			tools.ReadTopic(args[0], toBase64)
		},
	}

	cmdCheck = &cobra.Command{
		Use:              "check [from directory]",
		Short:            "check",
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

	cmdServer.Flags().BoolVarP(&useHttp, "http", "u", false, "config file (default is current directory)")
	cmdServer.Flags().Lookup("http").NoOptDefVal = "true"
	rootCmd.Flags().IntVarP(&serverPort, "port", "p", 50001, "config file (default is current directory)")
	rootCmd.Flags().StringVarP(&host, "host", "l", "localhost", "config file (default is current directory)")
	cmdServer.Flags().IntVarP(&maxBlockSizeMB, "maxBlockSize", "b", 100, "config file (default is current directory)")
	cmdServer.Flags().StringVarP(&cpuprofile, "cpuprofile", "c", "", "config file (default is current directory)")
	cmdServer.Flags().StringVarP(&memprofile, "memprofile", "m", "", "config file (default is current directory)")
	cmdCat.Flags().BoolVarP(&toBase64, "base64", "b", false, "Convert messages to base64")
	err := cmdServer.Flags().MarkHidden("cpuprofile")
	if err != nil {
		log.Fatal(err)
	}
	err = cmdServer.Flags().MarkHidden("memprofile")
	if err != nil {
		log.Fatal(err)
	}

	if useHttp && serverPort == 50001 {
		serverPort = 5001
	}
	rootCmd.AddCommand(cmdServer, cmdClient, cmdFile)
	cmdFile.AddCommand(cmdCat, cmdCheck)
	cmdClient.AddCommand(cmdClientCreate, cmdClientRead, cmdClientWrite, cmdClientBench)
}
