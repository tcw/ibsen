package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/cmd"
	"os"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
	zerolog.TimestampFieldName = "t"
	zerolog.LevelFieldName = "l"
	zerolog.MessageFieldName = "m"
	//path, err := os.Getwd()
	//logFile, err := os.Create(path + "/ibsen.log")
	//if err != nil {
	//	log.Fatal().Err(err)
	//}
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr}
	multi := zerolog.MultiLevelWriter(consoleWriter)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()
	cmd.Execute()
}
