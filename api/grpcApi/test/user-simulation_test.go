package test

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	go startGrpcServer()
	simulation, err := newSimulation(10, 10, 100000, time.Second*1)
	assert.Nil(t, err)
	simulation.start(t)
}
