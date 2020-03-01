package ext4

import (
	"log"
	"os"
)

func NewLogWriter(FileName string) *LogFile {
	lw := new(LogFile)
	lw.FileName = FileName

	f, err := os.OpenFile(lw.FileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Println(err)
	}
	lw.LogFile = f
	return lw
}

func (lw *LogFile) WriteToFile(logEntry LogEntry) int {
	bytes := append(logEntry.toLittleEndianOffest(), logEntry.toLittleEndianSize()...)
	bytes = append(bytes, logEntry.Payload...)
	n, err := lw.LogFile.Write(bytes)
	if err != nil {
		log.Println(err)
	}
	return n
}

func (lw *LogFile) CloseLogWriter() {
	lw.LogFile.Sync()
	lw.LogFile.Close()
}
