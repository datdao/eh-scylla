package ehscylla

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"

	eh "github.com/looplab/eventhorizon"
)

var ErrInvalidEventVersion = errors.New("invalid event version")

// ErrCouldNotMarshalEvent is when an event could not be marshaled into JSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// EventStore is an interface for an event sourcing event store.
type EventStore interface {
	// Save appends all events in the event stream to the store.
	Save(ctx context.Context, events []eh.Event, originalVersion int) error

	// Load loads all events for the aggregate id from the store.
	Load(ctx context.Context, aggregateId uuid.UUID, aggregateType eh.AggregateType, fromVersion int) ([]eh.Event, error)
}

type eventStore struct {
	session        *gocql.Session
	encoder        Encoder
	boundedContext string
}

func NewEventStore(session *gocql.Session, encoder Encoder, boundedContext string) (EventStore, error) {
	if encoder == nil {
		encoder = jsonEncoder{}
	}
	return &eventStore{
		session:        session,
		boundedContext: boundedContext,
		encoder:        encoder,
	}, nil
}

func (e *eventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	batch := e.session.NewBatch(gocql.LoggedBatch)
	for _, event := range events {
		// Marshal event data if there is any.
		data, err := e.encoder.Marshal(event.Data())
		if err != nil {
			return &eh.EventStoreError{
				Err: err,
			}
		}

		metadata, err := e.encoder.Marshal(event.Metadata())
		if err != nil {
			return &eh.EventStoreError{
				Err: err,
			}
		}

		batch.Query(`INSERT INTO event_store (
			bounded_context,
			aggregate_id,
			aggregate_type,
			event_id,
			event_type,
			event_version,
			event_data,
			event_timestamp,
			event_metadata)
		VALUES (?,?,?,?,?,?,?,?,?) IF NOT EXISTS;`, e.boundedContext, event.AggregateID().String(),
			event.AggregateType(), uuid.New().String(), event.EventType(), event.Version(), data, event.Timestamp(), metadata)
	}

	cas := make(map[string]interface{})
	applied, _, err := e.session.MapExecuteBatchCAS(batch, cas)
	if err != nil {
		return &eh.EventStoreError{
			Err: err,
		}
	}

	if !applied {
		return &eh.EventStoreError{
			Err: ErrInvalidEventVersion,
		}
	}

	return nil
}

// Load loads all events for the aggregate id from the store.
func (e *eventStore) Load(ctx context.Context, aggregateId uuid.UUID, aggregateType eh.AggregateType, fromVersion int) ([]eh.Event, error) {
	var events []eh.Event
	scanner := e.session.Query(`
	SELECT 
		aggregate_id,
		aggregate_type,
		event_id,
		event_type,
		event_version,
		event_data,
		event_timestamp,
		event_metadata
	FROM event_store WHERE 
	bounded_context = ? 
	AND aggregate_id = ?
	AND aggregate_type = ?
	AND event_version > ?`,
		e.boundedContext, aggregateId.String(), aggregateType, fromVersion).Consistency(gocql.One).WithContext(ctx).Iter().Scanner()

	for scanner.Next() {
		var aggregateId gocql.UUID
		var aggregateType eh.AggregateType
		var eventId gocql.UUID
		var eventType eh.EventType
		var eventVersion int
		var eventTimeStamp time.Time
		var eventData []byte
		var eventMetaData []byte

		err := scanner.Scan(
			&aggregateId,
			&aggregateType,
			&eventId,
			&eventType,
			&eventVersion,
			&eventData,
			&eventTimeStamp,
			&eventMetaData)
		if err != nil {
			return nil, &eh.EventStoreError{
				Err: err,
			}
		}

		eventDataUnmarshal, err := e.encoder.UnmarshalEventData(eventType, eventData)
		if err != nil {
			return nil, &eh.EventStoreError{
				Err: err,
			}
		}

		eventMetaDataUnmarshal, err := e.encoder.UnmarshalEventMetaData(eventMetaData)
		if err != nil {
			return nil, &eh.EventStoreError{
				Err: err,
			}
		}

		event := eh.NewEventForAggregate(
			eventType,
			eventDataUnmarshal,
			eventTimeStamp,
			aggregateType,
			uuid.UUID(aggregateId),
			eventVersion,
			eh.WithMetadata(eventMetaDataUnmarshal))

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, &eh.EventStoreError{
			Err: err,
		}
	}

	return events, nil
}

// Close closes the EventStore.
func (e *eventStore) Close() error {
	e.session.Close()
	return nil
}
