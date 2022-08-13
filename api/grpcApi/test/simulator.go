package test

import (
	"context"
	"fmt"
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

type SimulationParams struct {
	topics       int
	users        int
	dataLimit    int
	testDuration time.Duration
	useDelay     bool
	writeDelay   RandomizedTimeInterval
	entries      RandomizedSizeInterval
}

type Simulation struct {
	params  SimulationParams
	started time.Time
	wg      *sync.WaitGroup
	cancel  chan bool
	users   []*User
}

type User struct {
	name           string
	offsets        map[string]access.Offset
	topics         GlobalTopics
	ibsenClient    IbsenClient
	params         SimulationParams
	dataWriteLimit int
	dataWritten    int
}

type RandomizedTimeInterval struct {
	max time.Duration
	min time.Duration
}

type RandomizedSizeInterval struct {
	max int
	min int
}

func (t RandomizedTimeInterval) value() time.Duration {
	if t.max == 0 {
		return t.max
	}
	interval := rand.Intn(int(t.max)-int(t.min)) + int(t.min)
	return time.Duration(interval)
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

func newSimulation(params SimulationParams) (Simulation, error) {
	globalTopics := GlobalTopics{topics: createTopics(params.topics)}
	log.Info().
		Int("topics", params.topics).
		Int("users", params.users).
		Dur("test_time", params.testDuration).
		Msg("new_simulator")
	allUsers, err := newUsers(params.users, globalTopics, params)
	if err != nil {
		return Simulation{}, err
	}
	return Simulation{
		params: params,
		wg:     &sync.WaitGroup{},
		cancel: make(chan bool, params.users),
		users:  allUsers,
	}, nil
}

func (s *Simulation) start(t *testing.T) {
	users := s.users
	for _, user := range users {
		user.run(t, s.wg, s.cancel)
	}
	start := time.Now()
	for {
		if time.Until(start.Add(s.params.testDuration)) <= 0 {
			log.Info().Msg("---------------Stopping simulator!!!!-------------------")
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

func (u *User) run(t *testing.T, wg *sync.WaitGroup, cancel chan bool) {
	go func(wg *sync.WaitGroup, cancel chan bool) {
		wg.Add(1)
		topics := u.topics.topics
		readersStarted := false

		for {
			select {
			case <-cancel:
				log.Info().Msg(fmt.Sprintf("cancel received for %s, wrote %d bytes", u.name, u.dataWritten))
				wg.Done()
				return
			default:
				if u.dataWriteLimit > u.dataWritten {
					time.Sleep(u.params.writeDelay.value())
					u.write(t)
				}
				if !readersStarted {
					for _, topic := range topics {
						go u.read(t, topic)
					}
					readersStarted = true
				}
			}
		}
	}(wg, cancel)
}

func (u *User) write(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Minute)
	numberOfEntries := u.params.entries.value()
	randTopic := u.topics.randTopic()
	entryByteSize := 100
	entries := createInputEntries(randTopic, numberOfEntries, entryByteSize)
	_, err := u.ibsenClient.Client.Write(ctx, &entries)
	written := u.dataWritten + (numberOfEntries * entryByteSize)
	u.dataWritten = written
	if err != nil {
		log.Fatal().Err(err).Msg("Simulated write failed")

	}
}

func (u *User) read(t *testing.T, topic string) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Minute)
	var offset uint64 = 0
	entryStream, err := u.ibsenClient.Client.Read(ctx, &grpcApi.ReadParams{
		StopOnCompletion: false,
		Topic:            topic,
		Offset:           offset,
		BatchSize:        uint32(u.params.entries.value()),
	})
	assert.Nil(t, err)

	var expectedOffset int64 = 0
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
		//log.Debug().Uint64("firstEntry", in.Entries[0].Offset).
		//	Uint64("lastEntry", in.Entries[len(in.Entries)-1].Offset).
		//	Msg("simulator read")
		//log.Info().Msgf("Entries %d", len(entries))
		for _, entry := range entries {
			entriesRead = entriesRead + 1
			if expectedOffset != int64(entry.Offset) {
				log.Warn().
					Str("user", u.name).
					Str("topic", topic).
					Int("entriesRead", entriesRead).
					Uint64("start_offset", offset).
					Uint64("current_offset", entry.Offset).
					Int64("last_offset", expectedOffset).
					Msg("offset_out_of_order")
				t.Fail()
			}
			expectedOffset = int64(entry.Offset) + 1
		}
	}
}

func createTopics(topics int) []string {
	var genTopics []string
	for i := 0; i < topics; i++ {
		genTopics = append(genTopics, "topic_"+strconv.Itoa(i))
	}
	return genTopics
}

func newUsers(users int, globalTopics GlobalTopics, params SimulationParams) ([]*User, error) {
	var genUsers []*User
	for i := 0; i < users; i++ {
		user, err := newUser("user_"+strconv.Itoa(i), globalTopics, params)
		if err != nil {
			return nil, err
		}
		genUsers = append(genUsers, user)
	}
	return genUsers, nil
}

func newUser(username string, globalTopics GlobalTopics, params SimulationParams) (*User, error) {
	client, err := newIbsenClient(ibsenTestTarge)
	if err != nil {
		return nil, err
	}
	return &User{
		name:           username,
		ibsenClient:    client,
		offsets:        make(map[string]access.Offset),
		dataWriteLimit: params.dataLimit / params.users,
		topics:         globalTopics,
		params:         params,
	}, nil
}
