package locking

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/consensus"
	"github.com/tcw/ibsen/errore"
	"io"
	"os"
	"sync"
	"time"
)

// FileLock is the driven adapter satisfying the coordination port with a lease kept in a
// file on the shared filesystem.
var _ consensus.SingleIbsenWriterLock = &FileLock{}

type FileLock struct {
	afero        *afero.Afero
	lockFile     string
	reclaimLease time.Duration
	leaseTime    time.Duration
	uniqueId     string
	// mu serializes lease renewals with ReleaseLock; stop ends the renewals once released
	mu       *sync.Mutex
	stop     chan struct{}
	stopOnce *sync.Once
}

func NewFileLock(afero *afero.Afero, lockFile string, leaseTime time.Duration, waitFor time.Duration) FileLock {
	return FileLock{
		afero:        afero,
		lockFile:     lockFile,
		reclaimLease: waitFor,
		leaseTime:    leaseTime,
		uniqueId:     uuid.New().String(),
		mu:           &sync.Mutex{},
		stop:         make(chan struct{}),
		stopOnce:     &sync.Once{},
	}
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
		log.Err(err).Msgf("failed while checking if file %s exists", fl.lockFile)
		return false
	}
	if exists {
		fileLock, err := fl.afero.OpenFile(fl.lockFile, os.O_RDONLY, 0550)
		if err != nil {
			log.Err(err).Msgf("failed while opening lock file %s", fl.lockFile)
			return false
		}
		byteUUID, err := io.ReadAll(fileLock)
		if err != nil {
			ioErr := fileLock.Close()
			if ioErr != nil {
				log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
				return false
			}
			log.Err(err).Msgf("failed while reading lock file %s", fl.lockFile)
			return false
		}
		ioErr := fileLock.Close()
		if ioErr != nil {
			log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
			return false
		}
		lockUUID := string(byteUUID)
		if lockUUID != fl.uniqueId {
			modificationTime, err := fl.getFileModificationTime()
			if err != nil {
				log.Err(err).Msgf("unable to get modification time for file %s", fl.lockFile)
				return false
			}
			if modificationTime.Add(fl.leaseTime).Before(time.Now()) {
				fileLockAdder, err := fl.afero.OpenFile(fl.lockFile, os.O_WRONLY|os.O_TRUNC, 0660)
				if err != nil {
					log.Err(err).Msgf("failed while opening lock file %s", fl.lockFile)
					return false
				}
				_, err = fileLockAdder.Write([]byte(fl.uniqueId))
				ioErr := fileLockAdder.Close()
				if err != nil || ioErr != nil {
					log.Err(errore.WrapError(ioErr, err)).Msgf("failed while claiming expired lock file %s", fl.lockFile)
					return false
				}
				go fl.reclaimer()
				return true
			} else {
				return false
			}
		}
	} else {
		fileLockNew, err := fl.afero.OpenFile(fl.lockFile, os.O_RDWR|os.O_CREATE, 0660)
		if err != nil {
			log.Err(err).Msgf("failed while claiming lock file %s", fl.lockFile)
			return false
		}
		_, err = fileLockNew.Write([]byte(fl.uniqueId))
		if err != nil {
			ioErr := fileLockNew.Close()
			if ioErr != nil {
				log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
				return false
			}
			log.Err(err).Msgf("failed while writing to claim lock file %s", fl.lockFile)
			return false
		}
		go fl.reclaimer()
		ioErr := fileLockNew.Close()
		if ioErr != nil {
			log.Err(err).Msgf("failed while closing lock file %s", fl.lockFile)
			return false
		}
		return true
	}
	return false
}

func (fl FileLock) reclaimer() {
	for {
		select {
		case <-fl.stop:
			return
		case <-time.After(fl.reclaimLease):
		}
		if !fl.renewLease() {
			return
		}
	}
}

// renewLease rewrites the lock file to extend the lease, and reports false once the lock is
// released. A lease that cannot be renewed exits the process: another instance may claim the
// lock when the lease expires, and two writers would corrupt the log.
func (fl FileLock) renewLease() bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	select {
	case <-fl.stop:
		return false
	default:
	}
	// the lock file must already exist: O_EXCL without O_CREATE is ignored by Linux but
	// rejects an existing file on other filesystems, so it is not used here
	fileLock, err := fl.afero.OpenFile(fl.lockFile, os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatal().Err(err).Msgf("unable to renew single writer lock %s", fl.lockFile)
	}
	_, err = fileLock.Write([]byte(fl.uniqueId))
	ioErr := fileLock.Close()
	if err != nil {
		log.Fatal().Err(err).Msgf("unable to renew single writer lock %s", fl.lockFile)
	}
	if ioErr != nil {
		log.Printf("failed while closing lock file %s", fl.lockFile)
	}
	return true
}

func (fl FileLock) getFileModificationTime() (time.Time, error) {
	stat, err := fl.afero.Stat(fl.lockFile)
	if err != nil {
		return time.Time{}, err
	}
	return stat.ModTime(), nil
}

func (fl FileLock) ReleaseLock() bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.stopOnce.Do(func() { close(fl.stop) })
	err := fl.afero.Remove(fl.lockFile)
	if err != nil {
		log.Err(err).Msgf("failed to remove lock file [%s]", fl.lockFile)
		return false
	}
	log.Info().Msgf("Removed lockfile with id [%s]", fl.uniqueId)
	return true
}
