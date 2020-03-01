package ext4

import (
	"bufio"
	"os"
)

type LogFile struct {
	LogWriter *bufio.Writer
	LogFile   *os.File
	FileName  string
}
