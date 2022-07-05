package test

import (
	"context"
	"github.com/rs/zerolog/log"
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
	testTime       time.Duration
	started        time.Time
	wg             *sync.WaitGroup
	cancel         chan bool
	users          []*User
	writeDataLimit int
	dataWritten    int
}

func newSimulation(topics int, users int, dataLimit int, testTime time.Duration) (Simulation, error) {
	globalTopics := GlobalTopics{topics: createTopics(topics)}
	allUsers, err := createUsers(users, globalTopics)
	if err != nil {
		return Simulation{}, err
	}
	return Simulation{
		testTime:       testTime,
		wg:             &sync.WaitGroup{},
		cancel:         make(chan bool, users),
		users:          allUsers,
		writeDataLimit: dataLimit,
		dataWritten:    0,
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
	name          string
	offsets       map[string]access.Offset
	topics        GlobalTopics
	ibsenClient   IbsenClient
	writeCallFreq RandomizedTimeInterval
	readCallFreq  RandomizedTimeInterval
	entries       RandomizedSizeInterval
}

func (u *User) run(t *testing.T, wg *sync.WaitGroup, cancel chan bool) {
	go func(wg *sync.WaitGroup, cancel chan bool) {
		wg.Add(1)
		for {
			select {
			case <-cancel:
				log.Info().Msg("cancel received for user")
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
	if rand.Intn(10) > 5 {
		u.write(t)
	} else {
		u.read(t)
	}
}

func (u *User) write(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	numberOfEntries := u.entries.value()
	randTopic := u.topics.randTopic()
	entries := createInputEntries(randTopic, numberOfEntries, 100)
	_, err := u.ibsenClient.Client.Write(ctx, &entries)
	log.Info().Int("wrote", numberOfEntries).Str("topic", randTopic).Msg("Wrote to log")
	assert.Nil(t, err)
}

func (u *User) read(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
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
	entriesRead := 0
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
				log.Error()
			}
			assert.Equal(t, lastOffset+1, int64(entry.Offset), "user:", u.name, "current offset:", offset, "topic:", topic, "offset map:", u.offsets)

			lastOffset = int64(entry.Offset)
		}
	}
	u.offsets[topic] = access.Offset(lastOffset)
	log.Info().Int("read", entriesRead).Str("topic", topic).Uint64("offset", offset).Msg("Read from log")
}

func createTopics(topics int) []string {
	var genTopics []string
	for i := 0; i < topics; i++ {
		genTopics = append(genTopics, "topic"+strconv.Itoa(i))
	}
	return genTopics
}

func createUsers(users int, globalTopics GlobalTopics) ([]*User, error) {
	var genUsers []*User
	for i := 0; i < users; i++ {
		client, err := newIbsenClient(ibsenTestTarge)
		if err != nil {
			return nil, err
		}
		genUsers = append(genUsers, &User{
			name:        "user_" + strconv.Itoa(i),
			ibsenClient: client,
			offsets:     make(map[string]access.Offset),
			topics:      globalTopics,
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
		})
	}
	return genUsers, nil
}
