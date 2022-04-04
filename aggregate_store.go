package ehscylla

import (
	"context"
	"errors"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	ehEvents "github.com/looplab/eventhorizon/aggregatestore/events"
)

type Aggregate interface {
	ehEvents.VersionedAggregate

	SnapshotData() interface{}
}

type aggregateStore struct {
	eh.AggregateStore

	snapshot         AggregateSnapshot
	snapshotStrategy func(aggregate eh.Aggregate) bool
	store            EventStore
}

func NewAggregateStoreWithBoundedContext(
	session *gocql.Session,
	boundedContext string,
	encoder Encoder,
	snapshotStrategy func(aggregate eh.Aggregate) bool) (*aggregateStore, error) {

	if snapshotStrategy == nil {
		snapshotStrategy = StrategySnapshotDefault
	}

	eventStore, err := NewEventStore(session, encoder, boundedContext)
	if err != nil {
		return nil, err
	}

	snapshot, err := NewAggregateSnapshot(session, boundedContext)
	if err != nil {
		return nil, err
	}

	return &aggregateStore{
		store:            eventStore,
		snapshot:         snapshot,
		snapshotStrategy: snapshotStrategy,
	}, nil
}

// Load loads the most recent version of an aggregate with a type and id.
func (s *aggregateStore) Load(ctx context.Context, aggType eh.AggregateType, id uuid.UUID) (eh.Aggregate, error) {
	agg, err := eh.CreateAggregate(aggType, id)
	if err != nil {
		return nil, &eh.AggregateStoreError{
			Err:           err,
			Op:            eh.AggregateStoreOpLoad,
			AggregateType: aggType,
			AggregateID:   id,
		}
	}

	a, ok := agg.(ehEvents.VersionedAggregate)
	if !ok {
		return nil, &eh.AggregateStoreError{
			Err:           ehEvents.ErrAggregateNotVersioned,
			Op:            eh.AggregateStoreOpLoad,
			AggregateType: aggType,
			AggregateID:   id,
		}
	}

	if aggSnap, support := a.(Aggregate); support {
		_, err = s.snapshot.Restore(ctx, aggSnap)
		if err != nil {
			// Re-create aggregate if error
			agg, err = eh.CreateAggregate(aggType, id)
			if err != nil {
				return nil, err
			}
			a, _ = agg.(ehEvents.VersionedAggregate)
		}
	}

	events, err := s.store.Load(ctx, a.EntityID(), a.AggregateType(), a.AggregateVersion())
	if err != nil && !errors.Is(err, eh.ErrAggregateNotFound) {
		return nil, &eh.AggregateStoreError{
			Err:           err,
			Op:            eh.AggregateStoreOpLoad,
			AggregateType: aggType,
			AggregateID:   id,
		}
	}

	if err := s.applyEvents(ctx, a, events); err != nil {
		return nil, err
	}

	return a, nil
}

func (s *aggregateStore) applyEvents(ctx context.Context, a ehEvents.VersionedAggregate, events []eh.Event) error {
	for _, event := range events {
		if event.AggregateType() != a.AggregateType() {
			return ehEvents.ErrMismatchedEventType
		}

		if err := a.ApplyEvent(ctx, event); err != nil {
			return fmt.Errorf("could not apply event %s: %w", event, err)
		}

		a.SetAggregateVersion(event.Version())
	}

	return nil
}

func (r *aggregateStore) Save(ctx context.Context, agg eh.Aggregate) error {
	a, ok := agg.(ehEvents.VersionedAggregate)
	if !ok {
		return &eh.AggregateStoreError{
			Err:           ehEvents.ErrAggregateNotVersioned,
			Op:            eh.AggregateStoreOpSave,
			AggregateType: agg.AggregateType(),
			AggregateID:   agg.EntityID(),
		}
	}

	// Retrieve any new events to store.
	events := a.UncommittedEvents()
	if len(events) == 0 {
		return nil
	}

	if err := r.store.Save(ctx, events, a.AggregateVersion()); err != nil {
		return &eh.AggregateStoreError{
			Err:           err,
			Op:            eh.AggregateStoreOpSave,
			AggregateType: agg.AggregateType(),
			AggregateID:   agg.EntityID(),
		}
	}

	a.ClearUncommittedEvents()

	// Apply the events in case the aggregate needs to be further used
	// after this save. Currently it is not reused.
	if err := r.applyEvents(ctx, a, events); err != nil {
		return &eh.AggregateStoreError{
			Err:           err,
			Op:            eh.AggregateStoreOpSave,
			AggregateType: agg.AggregateType(),
			AggregateID:   agg.EntityID(),
		}
	}

	// Auto snapshot
	if r.snapshotStrategy(agg) {
		if err := r.snapshot.Store(ctx, agg); err != nil {
			return &eh.AggregateStoreError{
				Err:           err,
				Op:            eh.AggregateStoreOpSave,
				AggregateType: agg.AggregateType(),
				AggregateID:   agg.EntityID(),
			}
		}

	}

	return nil
}
