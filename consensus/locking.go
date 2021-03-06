package consensus

import (
	"github.com/google/uuid"
	"github.com/spf13/afero"
	"log"
	"os"
	"time"
)

type Lock interface {
	AcquireLock() bool
	ReleaseLock() bool
}

type FileLock struct {
	afero        *afero.Afero
	lockFilePath string
	waitFor      time.Duration
	uniqueId     string
}

type NoFileLock struct{}

func NewFileLock(afero *afero.Afero, lockFilePath string, waitFor time.Duration) FileLock {
	return FileLock{
		afero:        afero,
		lockFilePath: lockFilePath,
		waitFor:      waitFor,
		uniqueId:     uuid.New().String(),
	}
}

func (nfl NoFileLock) AcquireLock() bool {
	return true
}

func (nfl NoFileLock) ReleaseLock() bool {
	return true
}

func (fl FileLock) AcquireLock() bool {
	started := time.Now()
	for {
		if started.Add(fl.waitFor).Before(time.Now()) {
			return false
		}
		fileLock, err := fl.afero.OpenFile(fl.lockFilePath,
			os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
		if err != nil {
			log.Printf("Unable to acquire lock due to error: %s", err.Error())
			time.Sleep(time.Second * 1)
			continue
		}
		_, err = fileLock.Write([]byte(fl.uniqueId))
		if err != nil {
			log.Printf("Unable to acquire lock due to error: %s", err.Error())
			time.Sleep(time.Second * 1)
			continue
		}
		log.Printf("Got file lock with id [%s]\n", fl.uniqueId)
		return true
	}
}

func (fl FileLock) ReleaseLock() bool {
	err := fl.afero.Remove(fl.lockFilePath)
	if err != nil {
		log.Printf("failed to remove lock file [%s]", fl.lockFilePath)
		return false
	}
	log.Printf("Removed lockfile with id [%s]\n", fl.uniqueId)
	return true
}
