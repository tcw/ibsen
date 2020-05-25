package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	storagePath    string
	useHttp        bool
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
		Use:   "server [from directory]",
		Short: "start",
		Long:  `start`,
	}

	cmdServerStart = &cobra.Command{
		Use:   "start [data directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			absolutePath, err2 := filepath.Abs(args[1])
			if err2 != nil {
				log.Fatal(err2)
			}
			fmt.Println("Echo: " + absolutePath)
		},
	}

	cmdClient = &cobra.Command{
		Use:   "client [from directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdClientRead = &cobra.Command{
		Use:   "read [from directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdClientWrite = &cobra.Command{
		Use:   "write [from directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdClientBench = &cobra.Command{
		Use:   "start [from directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdCat = &cobra.Command{
		Use:   "cat [from file/directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
		},
	}

	cmdCheck = &cobra.Command{
		Use:   "check [from directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
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

	rootCmd.PersistentFlags().BoolVarP(&useHttp, "http1.1", "u", false, "config file (default is current directory)")
	rootCmd.PersistentFlags().IntVarP(&serverPort, "port", "p", 50001, "config file (default is current directory)")
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
	cmdClient.AddCommand(cmdClientRead, cmdClientWrite, cmdClientBench)
}
