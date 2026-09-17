package topic

import (
	"github.com/rs/zerolog/log"
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

type StatusAccess interface {
	List() []domain.TopicName
}

var _ StatusAccess = &Status{}

type Status struct {
	Store driven.BlockStore
}

func (s *Status) List() []domain.TopicName {
	topics, err := s.Store.Topics()
	if err != nil {
		log.Err(err).Msg("failed listing topics")
	}
	return topics
}
