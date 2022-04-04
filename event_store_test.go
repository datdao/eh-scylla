package ehscylla

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
)

func initSession() *gocql.Session {
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

	return session
}

func Test_eventStore_Save(t *testing.T) {
	session := initSession()
	defer session.Close()

	//testcase 1
	aggregateId1 := uuid.New()
	event1 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		map[string]string{"test": "1234"}, time.Now(), eh.AggregateType("order"), aggregateId1, 1)

	//testcase 2
	aggregateId2_1 := uuid.New()
	event2_1 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		map[string]string{"test": "1234"}, time.Now(), eh.AggregateType("order"), aggregateId2_1, 1)
	aggregateId2_2 := uuid.New()
	event2_2 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		map[string]string{"test": "1234"}, time.Now(), eh.AggregateType("order"), aggregateId2_2, 1)

	//testcase 3
	aggregateId3_1 := uuid.New()
	event3_1 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		map[string]string{"test": "1234"}, time.Now(), eh.AggregateType("order"), aggregateId3_1, 1)
	aggregateId3_2 := aggregateId3_1
	event3_2 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		map[string]string{"test": "1234"}, time.Now(), eh.AggregateType("order"), aggregateId3_2, 2)

	//testcase 5
	aggregateId5_1 := uuid.New()
	event5_1 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		map[string]string{"test": "1234"}, time.Now(), eh.AggregateType("order"), aggregateId5_1, 1)

	type fields struct {
		session        *gocql.Session
		encoder        Encoder
		boundedContext string
	}
	type args struct {
		ctx             context.Context
		events          []eh.Event
		originalVersion int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "insert 1 one event",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "save_event",
			},
			args: args{
				ctx:    context.Background(),
				events: []eh.Event{event1},
			},
			wantErr: false,
		},
		{
			name: "insert batch events diff partition",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "save_event",
			},
			args: args{
				ctx:    context.Background(),
				events: []eh.Event{event2_1, event2_2},
			},
			wantErr: true,
		},
		{
			name: "insert batch events same partition",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "save_event",
			},
			args: args{
				ctx:    context.Background(),
				events: []eh.Event{event3_1, event3_2},
			},
			wantErr: false,
		},
		{
			name: "insert existed event",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "save_event",
			},
			args: args{
				ctx:    context.Background(),
				events: []eh.Event{event1},
			},
			wantErr: true,
		},
		{
			name: "insert batch events (1 success, 1 fail)",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "save_event",
			},
			args: args{
				ctx:    context.Background(),
				events: []eh.Event{event5_1, event1},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &eventStore{
				session:        tt.fields.session,
				encoder:        tt.fields.encoder,
				boundedContext: tt.fields.boundedContext,
			}
			if err := e.Save(tt.args.ctx, tt.args.events, tt.args.originalVersion); (err != nil) != tt.wantErr {
				t.Errorf("eventStore.Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_eventStore_Load(t *testing.T) {
	session := initSession()
	defer session.Close()

	// testcase 1
	aggregateId1 := uuid.New()

	type eventData struct {
		Test string `json:"test"`
	}

	eh.RegisterEventData(eh.EventType("order:created"), func() eh.EventData { return &eventData{} })

	event1 := eh.NewEventForAggregate(
		eh.EventType("order:created"),
		&eventData{
			Test: "1",
		}, time.Now(), eh.AggregateType("order"), aggregateId1, 1, eh.WithMetadata(map[string]interface{}{"metadata": "1"}))

	// testcase 2
	aggregateId2 := uuid.New()
	events2 := []eh.Event{}

	for i := 1; i < 10; i++ {
		event := eh.NewEventForAggregate(
			eh.EventType("order:created"),
			&eventData{
				Test: fmt.Sprintf("%d", i),
			}, time.Now(), eh.AggregateType("order"), aggregateId2, i, eh.WithMetadata(map[string]interface{}{"metadata": fmt.Sprintf("%d", i)}))

		events2 = append(events2, event)
	}

	type fields struct {
		session        *gocql.Session
		encoder        Encoder
		boundedContext string
	}
	type args struct {
		ctx           context.Context
		uuid          uuid.UUID
		aggregateType eh.AggregateType
		fromVersion   int
	}
	tests := []struct {
		name      string
		fields    fields
		preInsert []eh.Event
		args      args
		want      []eh.Event
		wantErr   bool
	}{
		{
			name: "load 1 event",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "load_event",
			},
			preInsert: []eh.Event{event1},
			args: args{
				ctx:           context.Background(),
				uuid:          aggregateId1,
				aggregateType: event1.AggregateType(),
				fromVersion:   0,
			},
			want:    []eh.Event{event1},
			wantErr: false,
		},
		{
			name: "load events",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "load_event",
			},
			preInsert: events2,
			args: args{
				ctx:           context.Background(),
				uuid:          aggregateId2,
				aggregateType: event1.AggregateType(),
				fromVersion:   0,
			},
			want:    events2,
			wantErr: false,
		},
		{
			name: "load events from version",
			fields: fields{
				session:        session,
				encoder:        jsonEncoder{},
				boundedContext: "load_event",
			},
			args: args{
				ctx:           context.Background(),
				uuid:          aggregateId2,
				aggregateType: event1.AggregateType(),
				fromVersion:   5,
			},
			want:    events2[5:],
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &eventStore{
				session:        tt.fields.session,
				encoder:        tt.fields.encoder,
				boundedContext: tt.fields.boundedContext,
			}

			if len(tt.preInsert) > 0 {
				err := e.Save(tt.args.ctx, tt.preInsert, 0)
				if err != nil {
					t.Error("pre_insert error", err)
				}
			}

			got, err := e.Load(tt.args.ctx, tt.args.uuid, tt.args.aggregateType, tt.args.fromVersion)
			if (err != nil) != tt.wantErr {
				t.Errorf("eventStore.Load() error = %+v, wantErr %+v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Error("eventStore.Load() error: event length", len(got), len(tt.want))
				return
			}

			for i := 0; i < len(tt.want); i++ {
				if tt.want[i].AggregateID() != got[i].AggregateID() {
					t.Error("eventStore.Load() error: aggregate id")
					return
				}

				if tt.want[i].Version() != got[i].Version() {
					t.Error("eventStore.Load() error: version")
					return
				}

				if tt.want[i].AggregateType() != got[i].AggregateType() {
					t.Error("eventStore.Load() error: aggregate Type")
					return
				}

				if tt.want[i].EventType() != got[i].EventType() {
					t.Error("eventStore.Load() error: event_type")
					return
				}

				if tt.want[i].Timestamp().UTC().Format(time.RFC3339) != got[i].Timestamp().UTC().Format(time.RFC3339) {
					t.Error("eventStore.Load() error: timestamp")
					return
				}

				if !cmp.Equal(tt.want[i].Data(), got[i].Data()) {
					t.Error("eventStore.Load() error: event data", tt.want[i].Data(), got[i].Data())
					return
				}

				if !cmp.Equal(tt.want[i].Metadata(), got[i].Metadata()) {
					t.Error("eventStore.Load() error: event metadata", tt.want[i].Metadata(), got[i].Metadata())
					return
				}
			}

		})
	}
}
