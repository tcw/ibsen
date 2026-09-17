package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestName(t *testing.T) {
	startTestServer(t)
	//file, err := startCpuPprof()
	//assert.Nil(t, err)

	params := SimulationParams{
		topics:       3,
		users:        3,
		dataLimit:    10 * 1024 * 1024,
		testDuration: time.Second * 2,
		writeDelay: RandomizedTimeInterval{
			min: time.Millisecond * 1,
			max: time.Millisecond * 10,
		},
		entries: RandomizedSizeInterval{
			min: 1,
			max: 10000,
		},
	}

	simulation, err := newSimulation(params)
	assert.Nil(t, err)
	simulation.start(t)
}
