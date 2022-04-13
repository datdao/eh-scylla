package ehscylla

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gocql/gocql"
	eh "github.com/looplab/eventhorizon"
)

type AggregateSnapshotError struct {
	Err error
}

// Error implements the Error method of the error interface.
func (e *AggregateSnapshotError) Error() string {
	str := "aggregate snapshot: "

	if e.Err != nil {
		str += e.Err.Error()
	} else {
		str += "unknown error"
	}

	return str
}

var ErrUpdateSnapshot = errors.New("could not update snapshot")

type AggregateSnapshot interface {
	Restore(ctx context.Context, aggregate eh.Aggregate) (eh.Aggregate, error)
	Store(ctx context.Context, aggregate eh.Aggregate) error
}

type aggregateSnapshot struct {
	session        *gocql.Session
	boundedContext string
}

func NewAggregateSnapshot(session *gocql.Session, boundedContext string) (AggregateSnapshot, error) {
	return &aggregateSnapshot{
		session:        session,
		boundedContext: boundedContext,
	}, nil
}

func (a *aggregateSnapshot) Restore(ctx context.Context, aggregate eh.Aggregate) (eh.Aggregate, error) {
	aggSnapshotSupported, ok := aggregate.(Aggregate)
	if !ok {
		return aggregate, nil
	}

	var snapshotData string
	var snapshotVersion int
	var snapshotMetaData string

	err := a.session.Query(`
	SELECT 
		snapshot_data,
		snapshot_version,
		snapshot_metadata
	FROM aggregate_snapshot WHERE 
	bounded_context = ? 
	AND aggregate_id = ?
	AND aggregate_type = ?
	`,
		a.boundedContext, aggregate.EntityID().String(), aggregate.AggregateType()).Consistency(gocql.One).Scan(&snapshotData, &snapshotVersion, &snapshotMetaData)
	if err != nil {
		if err == gocql.ErrNotFound {
			return aggSnapshotSupported, nil
		}

		return aggSnapshotSupported, &AggregateSnapshotError{
			Err: err,
		}
	}

	err = json.Unmarshal([]byte(snapshotData), aggSnapshotSupported.SnapshotData())
	if err != nil {
		return aggSnapshotSupported, &AggregateSnapshotError{
			Err: err,
		}
	}

	// Increment Version to snapshot version
	aggSnapshotSupported.SetAggregateVersion(snapshotVersion)

	return aggSnapshotSupported, nil
}

func (a *aggregateSnapshot) Store(ctx context.Context, aggregate eh.Aggregate) error {
	aggSnapshotSupported, ok := aggregate.(Aggregate)
	if !ok {
		return nil
	}

	batch := a.session.NewBatch(gocql.LoggedBatch)
	data, err := json.Marshal(aggSnapshotSupported.SnapshotData())
	if err != nil {
		return &eh.AggregateStoreError{
			Err: err,
		}
	}

	batch.Query(`INSERT INTO aggregate_snapshot (
			bounded_context,
			aggregate_id,
			aggregate_type,
			snapshot_data,
			snapshot_version,
			snapshot_timestamp,
			snapshot_metadata)
		VALUES (?,?,?,?,?,?,?)`, a.boundedContext, aggregate.EntityID().String(),
		aggregate.AggregateType(), data, aggSnapshotSupported.AggregateVersion(), time.Now().UTC(), "")

	err = a.session.ExecuteBatch(batch)

	if err != nil {
		return &AggregateSnapshotError{
			Err: err,
		}
	}

	return nil
}
