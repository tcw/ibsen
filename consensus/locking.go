package consensus

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"io"
	"os"
	"time"
)

type Lock interface {
	AcquireLock() bool
	ReleaseLock() bool
}

type FileLock struct {
	afero        *afero.Afero
	lockFile     string
	reclaimLease time.Duration
	leaseTime    time.Duration
	uniqueId     string
}

type NoFileLock struct{}

func NewFileLock(afero *afero.Afero, lockFile string, leaseTime time.Duration, waitFor time.Duration) FileLock {
	return FileLock{
		afero:        afero,
		lockFile:     lockFile,
		reclaimLease: waitFor,
		leaseTime:    leaseTime,
		uniqueId:     uuid.New().String(),
	}
}

func (nfl NoFileLock) AcquireLock() bool {
	return true
}

func (nfl NoFileLock) ReleaseLock() bool {
	return true
}

// Does file exist?
//	true -> is uuid owner?
//		true -> extend lease claim
//		false -> has lease expired?
//			true -> claim lock with new uuid (start lease update)
//			false -> backoff
// false -> claim lock with new uuid (start lease update)

func (fl FileLock) AcquireLock() bool {
	exists, err := fl.afero.Exists(fl.lockFile)
	if err != nil {
		log.Printf("failed while checking if file %s exists", fl.lockFile)
		return false
	}
	if exists {
		fileLock, err := fl.afero.OpenFile(fl.lockFile, os.O_RDONLY, 0550)
		if err != nil {
			log.Printf("failed while opening lock file %s", fl.lockFile)
			return false
		}
		byteUUID, err := io.ReadAll(fileLock)
		if err != nil {
			ioErr := fileLock.Close()
			if ioErr != nil {
				log.Printf("failed while closing lock file %s", fl.lockFile)
				return false
			}
			log.Printf("failed while reading lock file %s", fl.lockFile)
			return false
		}
		ioErr := fileLock.Close()
		if ioErr != nil {
			log.Printf("failed while closing lock file %s", fl.lockFile)
			return false
		}
		lockUUID := string(byteUUID)
		if lockUUID != fl.uniqueId {
			modificationTime, err := fl.getFileModificationTime()
			if err != nil {
				log.Printf("unable to get modification time for file %s", fl.lockFile)
				return false
			}
			if modificationTime.Add(fl.leaseTime).Before(time.Now()) {
				fileLockAdder, err := fl.afero.OpenFile(fl.lockFile, os.O_WRONLY|os.O_TRUNC, 0660)
				if err != nil {
					log.Printf("failed while opening lock file %s", fl.lockFile)
					return false
				}
				_, err = fileLockAdder.Write([]byte(fl.uniqueId))
				go fl.reclaimer()
				return true
			} else {
				return false
			}
		}
	} else {
		fileLockNew, err := fl.afero.OpenFile(fl.lockFile, os.O_RDWR|os.O_CREATE, 0660)
		if err != nil {
			log.Printf("failed while claiming lock file %s", fl.lockFile)
			return false
		}
		_, err = fileLockNew.Write([]byte(fl.uniqueId))
		if err != nil {
			ioErr := fileLockNew.Close()
			if ioErr != nil {
				log.Printf("failed while closing lock file %s", fl.lockFile)
				return false
			}
			log.Printf("failed while writing to claim lock file %s", fl.lockFile)
			return false
		}
		go fl.reclaimer()
		ioErr := fileLockNew.Close()
		if ioErr != nil {
			log.Printf("failed while closing lock file %s", fl.lockFile)
			return false
		}
		return true
	}
	return false
}

func (fl FileLock) reclaimer() {
	time.Sleep(fl.reclaimLease)

	for {
		fileLock, err := fl.afero.OpenFile(fl.lockFile, os.O_RDWR|os.O_EXCL, 0660)
		if err != nil {
			log.Fatal().Err(err)
		}
		_, err = fileLock.Write([]byte(fl.uniqueId))
		ioErr := fileLock.Close()
		if ioErr != nil {
			log.Printf("failed while closing lock file %s", fl.lockFile)
		}
		time.Sleep(fl.reclaimLease)
	}
}

func (fl FileLock) getFileModificationTime() (time.Time, error) {
	stat, err := fl.afero.Stat(fl.lockFile)
	if err != nil {
		return time.Time{}, err
	}
	return stat.ModTime(), nil
}

func (fl FileLock) ReleaseLock() bool {
	err := fl.afero.Remove(fl.lockFile)
	if err != nil {
		log.Printf("failed to remove lock file [%s]", fl.lockFile)
		return false
	}
	log.Printf("Removed lockfile with id [%s]\n", fl.uniqueId)
	return true
}
