package ext4

import (
	"github.com/tcw/ibsen/logStorage"
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

func (lw *LogFile) WriteToFile(logEntry *logStorage.LogEntry) int {
	bytes := append(offsetToLittleEndian(logEntry.Offset), byteSizeToLittleEndian(logEntry.ByteSize)...)
	bytes = append(bytes, logEntry.Entry...)
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
