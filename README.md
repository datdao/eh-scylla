# EventHorizon Scylla
Build scalable eventstore with scylla

## Features
### Multi bounded-context
Take advantage of NoSQL. We can use event store as a single source of truth throughout all contexts in your domain without scaling issues.
### Partitioning vs. Sharding
To improve query performance, the data is partitioned and sharded based on the ***bounded_context***, ***aggregate_id***, and ***aggregate_type*** columns, also sorted by ***event_version*** column; this means that events belonging to one aggregate always live in a specific node.
### Transaction
By using LTW (Lightweight transaction), consistency level, and batch query, we archive the **Atomicity** property on a single partition.
### Snapshot
Snapshot logic is triggered when the aggregate store saves or loads events. Furthermore, you can customize the snapshot strategy.
### Compaction
Event_store table uses [Size-tiered compaction strategy](https://docs.scylladb.com/kb/compaction/#size-tiered-compaction-strategy-stcs) to improve read speed.

Aggregate_snapshot table uses [Leveled compaction strategy](https://docs.scylladb.com/kb/compaction/#leveled-compaction-strategy-lcs) to reduce disk size and improve read speed.

## Installation

```bash
go get github.com/datdao/eh-scylla
```

## Usage

### Run migration script
```sql
CREATE KEYSPACE event_sourcing WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3};

CREATE TABLE event_store (bounded_context varchar, aggregate_id uuid, aggregate_type varchar, event_id uuid, event_type varchar, event_version int, event_data varchar, event_timestamp timestamp, event_metadata varchar, PRIMARY KEY ((bounded_context, aggregate_id, aggregate_type), event_version));

CREATE TABLE aggregate_snapshot (bounded_context varchar, aggregate_id uuid, aggregate_type varchar, snapshot_data varchar, snapshot_version int, snapshot_timestamp timestamp, snapshot_metadata varchar, PRIMARY KEY ((bounded_context, aggregate_id, aggregate_type))) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };
```

### Create AggregateStore

```golang
hosts := strings.Split(os.Getenv("SCYLLA_HOSTS"), ",")
username := os.Getenv("SCYLLA_USERNAME")
password := os.Getenv("SCYLLA_PASSWORD")
hostSelectionPolicy := os.Getenv("SCYLLA_HOSTSELECTIONPOLICY")

var cluster = gocql.NewCluster(hosts...)
cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(hostSelectionPolicy)
cluster.Keyspace = "event_sourcing"
cluster.Consistency = gocql.Quorum

// Create gocql cluster.
session, err := cluster.CreateSession()
if err != nil {
    panic(err)
}

// Create aggreagate store
aggStore, err := ehScylla.NewAggregateStoreWithBoundedContext(session, "order", nil, nil)
if err != nil {
    panic(err)
}

```

### Custom Snapshot strategy

```golang
var StrategySnapshotCustom = func(aggregate eh.Aggregate) bool {
	return aggregate.EntityID().Version()%20 == 0
}

ehScylla.NewAggregateStoreWithBoundedContext(session, "order", nil, StrategySnapshotCustom)
```