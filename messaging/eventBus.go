package messaging

import (
	"log"
	"sync"
)

const TopicChangeEventType = "topic:change"

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
	debugSubscribers DataChannelSlice
	subscribers      map[string]DataChannelSlice
	rm               sync.RWMutex
}

func NewEventBus() EventBus {
	return EventBus{
		debugSubscribers: DataChannelSlice{},
		subscribers:      map[string]DataChannelSlice{},
	}
}

var GlobalEventBus = NewEventBus()

func (b *EventBus) Publish(event Event) {
	publishOnEventBus(b, event)
}

func (b *EventBus) Subscribe(topic string, ch DataChannel) {
	subscribeOnEventBus(b, topic, ch)
}

func (b *EventBus) SubscribeAll(ch DataChannel) {
	subscribeAllOnEventBus(b, ch)
}

func Publish(event Event) {
	publishOnEventBus(&GlobalEventBus, event)
}

func Subscribe(topic string, ch DataChannel) {
	subscribeOnEventBus(&GlobalEventBus, topic, ch)
}

func SubscribeAll(ch DataChannel) {
	subscribeAllOnEventBus(&GlobalEventBus, ch)
}

func publishOnEventBus(eb *EventBus, event Event) {
	eb.rm.RLock()
	if chans, found := eb.subscribers[event.Type]; found {
		channels := append(DataChannelSlice{}, chans...)
		go func(data Event, dataChannelSlices DataChannelSlice) {
			for _, ch := range dataChannelSlices {
				ch <- data
			}
		}(Event{Data: event.Data, Type: event.Type}, channels)
	}
	if len(eb.debugSubscribers) > 0 {
		debugChannels := append(DataChannelSlice{}, eb.debugSubscribers...)
		go func(data Event, dataChannelSlices DataChannelSlice) {
			for _, ch := range dataChannelSlices {
				ch <- data
			}
		}(Event{Data: event.Data, Type: event.Type}, debugChannels)
	}
	eb.rm.RUnlock()
}

func subscribeOnEventBus(eb *EventBus, topic string, ch DataChannel) {
	eb.rm.Lock()
	if prev, found := eb.subscribers[topic]; found {
		eb.subscribers[topic] = append(prev, ch)
	} else {
		eb.subscribers[topic] = append([]DataChannel{}, ch)
	}
	eb.rm.Unlock()
}

func subscribeAllOnEventBus(eb *EventBus, ch DataChannel) {
	eb.rm.Lock()
	eb.debugSubscribers = append(eb.debugSubscribers, ch)
	eb.rm.Unlock()
}

func StartGlobalEventDebugging() {
	log.Println("started eventbus logging")
	debugChannel := make(chan Event)
	GlobalEventBus.SubscribeAll(debugChannel)
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
