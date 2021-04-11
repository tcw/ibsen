package messaging

import (
	"log"
)

const TopicChangeEventType = "topic:change"
const TopicCreatedEventType = "topic:created"
const TopicDroppedEventType = "topic:dropped"

type Event struct {
	Data interface{}
	Type string
}

type TopicChange struct {
	Topic  string
	Offset uint64
}

type DataChannel chan Event

type DataChannelSlice []DataChannel

type EventBus struct {
	subscribers DataChannelSlice
}

func NewEventBus() EventBus {
	return EventBus{
		subscribers: DataChannelSlice{},
	}
}

var GlobalEventBus = NewEventBus()

func (b *EventBus) Publish(event Event) {
	publishOnEventBus(b, event)
}

func (b *EventBus) Subscribe(ch DataChannel) {
	subscribeOnEventBus(b, ch)
}

func Publish(event Event) {
	publishOnEventBus(&GlobalEventBus, event)
}

func Subscribe(ch DataChannel) {
	subscribeOnEventBus(&GlobalEventBus, ch)
}

func publishOnEventBus(eb *EventBus, event Event) {
	channels := append(DataChannelSlice{}, eb.subscribers...)
	for _, ch := range channels {
		select {
		case ch <- event:
		default:
		}
	}
}

func subscribeOnEventBus(eb *EventBus, ch DataChannel) {
	eb.subscribers = append(eb.subscribers, ch)
}

func StartGlobalEventDebugging() {
	log.Println("started eventbus logging")
	debugChannel := make(chan Event)
	GlobalEventBus.Subscribe(debugChannel)
	go debugEventsToLog(debugChannel)
}

func StopGlobalEventDebugging() {
	//Todo: implement
}

func debugEventsToLog(logChan chan Event) {
	for {
		log.Println("Debug event -> ", <-logChan)
	}
}
