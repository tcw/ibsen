package ext4

import (
	"github.com/tcw/ibsen/logStorage"
	"os"
)

func NewLogWriter(FileName string) (*LogFile, error) {
	lw := new(LogFile)
	lw.FileName = FileName

	f, err := os.OpenFile(lw.FileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	lw.LogFile = f
	return lw, nil
}

func (lw *LogFile) WriteBatchToFile(logEntry *logStorage.LogBatchEntry) (uint64, int, error) {
	var bytes []byte
	var currentOffset uint64
	for i, v := range *logEntry.Entries {
		currentOffset = logEntry.Offset + uint64(i)
		bytes = append(bytes, offsetToLittleEndian(currentOffset)...)
		bytes = append(bytes, byteSizeToLittleEndian(len(v))...)
		bytes = append(bytes, v...)
	}
	n, err := lw.LogFile.Write(bytes)
	if err != nil {
		return 0, 0, err
	}
	return currentOffset, n, nil
}

func (lw *LogFile) WriteToFile(logEntry *logStorage.LogEntry) (int, error) {
	bytes := append(offsetToLittleEndian(logEntry.Offset), byteSizeToLittleEndian(logEntry.ByteSize)...)
	bytes = append(bytes, logEntry.Entry...)
	n, err := lw.LogFile.Write(bytes)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (lw *LogFile) CloseLogWriter() error {
	err := lw.LogFile.Sync()
	if err != nil {
		return err
	}
	return lw.LogFile.Close()
}
