package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var (
	storagePath    string
	useHttp        bool
	grpcPort       int
	httpPort       int
	maxBlockSizeMB int
	cpuprofile     bool
	memprofile     bool
	ibsenFiglet    = `
                           _____ _                    
                          |_   _| |                   
                            | | | |__  ___  ___ _ __  
                            | | | '_ \/ __|/ _ \ '_ \ 
                           _| |_| |_) \__ \  __/ | | |
                          |_____|_.__/|___/\___|_| |_|

	'One should not read to devour, but to see what can be applied.'
	 Henrik Ibsen (1828–1906)

`

	rootCmd = &cobra.Command{
		Use:   "ibsen",
		Short: "Ibsen is a kurbernetes friendly streaming platform",
		Long: `Ibsen builds on the shoulders of giants. Taking advantage of the recent advances in 
			distributed block storage and unix's philosophy of simplicity'`,
	}

	cmdServer = &cobra.Command{
		Use:   "start [from directory]",
		Short: "start",
		Long:  `start`,
	}

	cmdServerStart = &cobra.Command{
		Use:   "start [data directory]",
		Short: "start",
		Long:  `start`,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Echo: " + strings.Join(args, " "))
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

	rootCmd.PersistentFlags().StringVarP(&storagePath, "path", "p", "", "config file (default is current directory)")
	rootCmd.PersistentFlags().BoolVarP(&useHttp, "http", "u", false, "config file (default is current directory)")
	rootCmd.PersistentFlags().BoolVarP(&cpuprofile, "cpuprofile", "c", false, "config file (default is current directory)")
	rootCmd.PersistentFlags().BoolVarP(&memprofile, "memprofile", "m", false, "config file (default is current directory)")
	rootCmd.PersistentFlags().IntVarP(&grpcPort, "grpcPort", "g", 50001, "config file (default is current directory)")
	rootCmd.PersistentFlags().IntVarP(&httpPort, "httpPort", "h", 5001, "config file (default is current directory)")
	rootCmd.PersistentFlags().IntVarP(&maxBlockSizeMB, "maxBlockSize", "b", 100, "config file (default is current directory)")

	rootCmd.AddCommand(cmdServer, cmdClient, cmdCat, cmdCheck)
	cmdServer.AddCommand(cmdServerStart)
	cmdClient.AddCommand(cmdClientRead, cmdClientWrite, cmdClientBench)
}
