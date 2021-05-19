package smq
// what a simple message queue could look like. We can use a queue to reduce the amount of writes to the DB
// flush at certain intervals

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/mailgun/service/models"
	"sync"
	"time"
)
const(
	DefaultDuration = time.Duration(15) * time.Second
)

var simpleMessageQueue *MessageQueue

var (
	smqInit sync.Once
)


type MessageQueue struct {
	mu             sync.Mutex
	CounterMap     map[string]*models.Event
	sendTicker     *time.Ticker
	db *pgx.Conn
}

func Default(db *pgx.Conn) *MessageQueue {
	smqInit.Do(func() {
		simpleMessageQueue = simpleMessageQueue.NewMessageQueue(db)
	})
	return simpleMessageQueue
}


func (m *MessageQueue) NewMessageQueue(db *pgx.Conn ) *MessageQueue {

	ticker := time.NewTicker(DefaultDuration)
	go func() {
		for _ = range ticker.C{
				m.Flush()
		}
	}()

	return &MessageQueue{
		CounterMap:     map[string]*models.Event{},
		sendTicker:     ticker,
		db: db,
	}
}

func (m *MessageQueue) Add(event models.Event) {
	m.mu.Lock()
	if currentEvent, ok := m.CounterMap[event.Domain]; ok {
		currentEvent.Bounced += event.Bounced
		currentEvent.Delivered += event.Delivered
	}else{
		newEvent := &models.Event{
			Domain: event.Domain,
			Delivered: event.Delivered,
			Bounced: event.Bounced,
		}
		m.CounterMap[event.Domain] = newEvent
	}
	m.mu.Unlock()
	fmt.Printf("v ==== %v \n", m.CounterMap)
}

func (m *MessageQueue) Flush(){
	//	call the database writes here
}

