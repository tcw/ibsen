package test

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/tcw/ibsen/access"
	"github.com/tcw/ibsen/api/grpcApi"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// User
// Topics
// Read/Write
// Frequency
// Test time
// Max data to write

type RandomizedTimeInterval struct {
	max time.Duration
	min time.Duration
}

func (t RandomizedTimeInterval) value() time.Duration {
	if t.max == 0 {
		return t.max
	}
	interval := rand.Intn(int(t.max)-int(t.min)) + int(t.min)
	return time.Duration(interval)
}

type RandomizedSizeInterval struct {
	max int
	min int
}

func (s RandomizedSizeInterval) value() int {
	return rand.Intn(s.max-s.min) + s.min
}

type GlobalTopics struct {
	topics []string
}

func (g GlobalTopics) randTopic() string {
	return g.topics[rand.Intn(len(g.topics))]
}

type Simulation struct {
	testTime time.Duration
	started  time.Time
	wg       *sync.WaitGroup
	cancel   chan bool
	users    []*User
}

func newSimulation(afero *afero.Afero, topics int, users int, dataLimitInMB int, testTime time.Duration) (Simulation, error) {
	globalTopics := GlobalTopics{topics: createTopics(topics)}
	userDataLimit := dataLimitInMB * 1024 * 1024 / users
	log.Info().
		Int("topics", topics).
		Int("users", users).
		Dur("test_time", testTime).
		Int("data_limit_pr_user", userDataLimit).
		Msg("new_simulator")
	allUsers, err := newUsers(afs, users, globalTopics, userDataLimit)
	if err != nil {
		return Simulation{}, err
	}
	return Simulation{
		testTime: testTime,
		wg:       &sync.WaitGroup{},
		cancel:   make(chan bool, users),
		users:    allUsers,
	}, nil
}

func (s *Simulation) start(t *testing.T) {
	users := s.users
	for _, user := range users {
		user.run(t, s.wg, s.cancel)
	}
	start := time.Now()
	for {
		if time.Until(start.Add(s.testTime)) <= 0 {
			log.Info().Msg("Stopping simulator")
			s.stop()
			break
		}
		time.Sleep(100)
	}
}

func (s *Simulation) stop() {
	for i := 0; i < len(s.users); i++ {
		s.cancel <- true
	}
	s.wg.Wait()
}

type User struct {
	name           string
	offsets        map[string]access.Offset
	topics         GlobalTopics
	afs            *afero.Afero
	ibsenClient    IbsenClient
	writeCallFreq  RandomizedTimeInterval
	readCallFreq   RandomizedTimeInterval
	entries        RandomizedSizeInterval
	dataWriteLimit int
	dataWritten    int
}

func (u *User) run(t *testing.T, wg *sync.WaitGroup, cancel chan bool) {
	go func(wg *sync.WaitGroup, cancel chan bool) {
		wg.Add(1)
		for {
			select {
			case <-cancel:
				log.Info().Msg(fmt.Sprintf("cancel received for %s, wrote %d bytes", u.name, u.dataWritten))
				wg.Done()
				return
			default:
				time.Sleep(u.writeCallFreq.value())
				u.runSimulation(t)
			}
		}
	}(wg, cancel)
}

func (u *User) runSimulation(t *testing.T) {
	if rand.Intn(10) > 4 && u.dataWriteLimit > u.dataWritten {
		u.write(t)
	} else {
		u.read(t)
	}
}

func (u *User) write(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Minute)
	numberOfEntries := u.entries.value()
	randTopic := u.topics.randTopic()
	entryByteSize := 100
	entries := createInputEntries(randTopic, numberOfEntries, entryByteSize)
	_, err := u.ibsenClient.Client.Write(ctx, &entries)
	written := u.dataWritten + (numberOfEntries * entryByteSize)
	u.dataWritten = written
	//log.Info().Int("wrote", numberOfEntries).Str("topic", randTopic).Msg("read/write")
	assert.Nil(t, err)
}

func (u *User) read(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Minute)
	topic := u.topics.randTopic()
	var offset uint64 = 0
	if val, ok := u.offsets[topic]; ok {
		offset = uint64(val)
	}
	entryStream, err := u.ibsenClient.Client.Read(ctx, &grpcApi.ReadParams{
		StopOnCompletion: true,
		Topic:            topic,
		Offset:           offset,
		BatchSize:        uint32(u.entries.value()),
	})
	assert.Nil(t, err)

	var lastOffset int64 = -1
	entriesRead := int(u.offsets[topic])
	for {
		in, err := entryStream.Recv()
		if err == io.EOF {
			break
		}
		if in == nil {
			break
		}
		entries := in.Entries
		for _, entry := range entries {
			entriesRead = entriesRead + 1
			if lastOffset == -1 {
				lastOffset = int64(entry.Offset)
				continue
			}
			if lastOffset+1 != int64(entry.Offset) {
				log.Warn().
					Str("user", u.name).
					Str("topic", topic).
					Uint64("start_offset", offset).
					Uint64("current_offset", entry.Offset).
					Int64("last_offset", lastOffset).
					Msg("offset_out_of_order")
				t.Fail()
			}
			lastOffset = int64(entry.Offset)
		}
	}
	if lastOffset >= 0 {
		u.offsets[topic] = access.Offset(lastOffset)
	}
	//log.Info().Int("read", entriesRead).Str("topic", topic).Uint64("offset", offset).Msg("read/write")
}

func createTopics(topics int) []string {
	var genTopics []string
	for i := 0; i < topics; i++ {
		genTopics = append(genTopics, "topic_"+strconv.Itoa(i))
	}
	return genTopics
}

func newUsers(afs *afero.Afero, users int, globalTopics GlobalTopics, dataLimit int) ([]*User, error) {
	var genUsers []*User
	for i := 0; i < users; i++ {
		user, err := newUser("user_"+strconv.Itoa(i), globalTopics, dataLimit, afs)
		if err != nil {
			return nil, err
		}
		genUsers = append(genUsers, user)
	}
	return genUsers, nil
}

func newUser(username string, globalTopics GlobalTopics, datalimit int, afs *afero.Afero) (*User, error) {
	client, err := newIbsenClient(ibsenTestTarge)
	if err != nil {
		return nil, err
	}
	return &User{
		name:           username,
		afs:            afs,
		ibsenClient:    client,
		dataWriteLimit: datalimit,
		offsets:        make(map[string]access.Offset),
		topics:         globalTopics,
		writeCallFreq: RandomizedTimeInterval{
			min: time.Millisecond * 10,
			max: time.Millisecond * 100,
		},
		readCallFreq: RandomizedTimeInterval{
			min: time.Millisecond * 10,
			max: time.Millisecond * 100,
		},
		entries: RandomizedSizeInterval{
			min: 1,
			max: 1000,
		},
	}, nil
}
