package ehscylla

import eh "github.com/looplab/eventhorizon"

var StrategySnapshotDefault = func(aggregate eh.Aggregate) bool {
	return aggregate.EntityID().Version()%5 == 0
}
