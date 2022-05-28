package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/tcw/ibsen/cmd"
	"os"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimestampFieldName = "t"
	zerolog.LevelFieldName = "l"
	zerolog.MessageFieldName = "m"
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}
	//path, err := os.Getwd()
	//logFile, err := os.Create(path + "/ibsen.log")
	//if err != nil {
	//	log.Fatal().Err(err)
	//}
	multi := zerolog.MultiLevelWriter(consoleWriter)
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

	cmd.Execute()
}
