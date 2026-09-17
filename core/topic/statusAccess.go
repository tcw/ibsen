package topic

import (
	"github.com/tcw/ibsen/core/domain"
	"github.com/tcw/ibsen/core/port/driven"
)

type StatusAccess interface {
	List() []domain.TopicName
}

var _ StatusAccess = &Status{}

type Status struct {
	Store driven.BlockStore
	Log   driven.Logger
}

func (s *Status) List() []domain.TopicName {
	topics, err := s.Store.Topics()
	if err != nil {
		if s.Log != nil {
			s.Log.Log(driven.LevelError, "failed listing topics", driven.Err(err))
		}
	}
	return topics
}
