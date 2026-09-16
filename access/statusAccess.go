package access

import (
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/access/common"
)

type StatusAccess interface {
	List() []common.TopicName
}

var _ StatusAccess = &Status{}

type Status struct {
	Store common.BlockStore
}

func (s *Status) List() []common.TopicName {
	topics, err := s.Store.Topics()
	if err != nil {
		log.Err(err).Msg("failed listing topics")
	}
	return topics
}
