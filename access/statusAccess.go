package access

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"github.com/tcw/ibsen/access/common"
	ibsLog "github.com/tcw/ibsen/access/log"
)

type StatusAccess interface {
	List() []common.TopicName
}

var _ StatusAccess = &Status{}

type Status struct {
	Afs      *afero.Afero
	RootPath string
}

func (s *Status) List() []common.TopicName {
	topics, err := ibsLog.ListAllTopics(s.Afs, s.RootPath)
	if err != nil {
		log.Err(err).Msg("failed listing topics")
	}
	var topicNames []common.TopicName
	for _, topic := range topics {
		topicNames = append(topicNames, common.TopicName(topic))
	}
	return topicNames
}
