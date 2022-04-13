package ehscylla

import (
	"context"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/aggregatestore/events"
)

type aggregateTest struct {
	*events.AggregateBase

	data []string
}

func (aggregateTest) HandleCommand(context.Context, eh.Command) error {
	return nil
}

func (a *aggregateTest) SnapshotData() interface{} {
	return &a.data
}

func (a *aggregateTest) ApplyEvent(context.Context, eh.Event) error {
	return nil
}

func Test_aggregateSnapshot_Save(t *testing.T) {
	session := initSession()
	defer session.Close()

	type fields struct {
		session        *gocql.Session
		boundedContext string
	}
	type args struct {
		ctx       context.Context
		aggregate eh.Aggregate
	}

	// testcase 1
	aggregate1 := uuid.New()
	aggregateBase1 := events.NewAggregateBase(eh.AggregateType("order_snapshot"), aggregate1)
	aggregateBase1.SetAggregateVersion(1)

	// testcase 2
	aggregate2 := uuid.New()
	aggregateBase2 := events.NewAggregateBase(eh.AggregateType("order_snapshot"), aggregate2)
	aggregateBase2.SetAggregateVersion(1)

	aggregateBase2_1 := events.NewAggregateBase(eh.AggregateType("order_snapshot"), aggregate2)
	aggregateBase2_1.SetAggregateVersion(2)

	tests := []struct {
		name      string
		fields    fields
		args      args
		preInsert []eh.Aggregate
		wantErr   bool
	}{
		{
			name: "insert new snapshot",
			fields: fields{
				session:        session,
				boundedContext: "snapshot",
			},
			args: args{
				ctx: context.Background(),
				aggregate: &aggregateTest{
					AggregateBase: aggregateBase1,
					data:          []string{"1", "2", "3"},
				},
			},
			wantErr: false,
		},
		{
			name: "update  snapshot",
			fields: fields{
				session:        session,
				boundedContext: "snapshot",
			},
			preInsert: []eh.Aggregate{
				&aggregateTest{
					AggregateBase: aggregateBase2,
					data:          []string{"1", "2"},
				},
			},
			args: args{
				ctx: context.Background(),
				aggregate: &aggregateTest{
					AggregateBase: aggregateBase2_1,
					data:          []string{"1", "2", "3"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			a := &aggregateSnapshot{
				session:        tt.fields.session,
				boundedContext: tt.fields.boundedContext,
			}

			if len(tt.preInsert) > 0 {
				for _, agg := range tt.preInsert {
					err := a.Store(tt.args.ctx, agg)
					if err != nil {
						t.Error("pre_insert error", err)
					}
				}

			}

			if err := a.Store(tt.args.ctx, tt.args.aggregate); (err != nil) != tt.wantErr {
				t.Errorf("aggregateSnapshot.Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_aggregateSnapshot_Restore(t *testing.T) {
	session := initSession()
	defer session.Close()

	type fields struct {
		session        *gocql.Session
		boundedContext string
	}
	type args struct {
		ctx       context.Context
		aggregate eh.Aggregate
	}

	// testcase 1
	aggregate1 := uuid.New()
	aggregateBase1 := events.NewAggregateBase(eh.AggregateType("order_snapshot"), aggregate1)
	aggregateBase1.SetAggregateVersion(1)

	// testcase2
	aggregate2_1 := uuid.New()
	aggregateBase2_1 := events.NewAggregateBase(eh.AggregateType("order_snapshot"), aggregate2_1)
	aggregateBase2_1.SetAggregateVersion(2)

	aggregateBase2_2 := events.NewAggregateBase(eh.AggregateType("order_snapshot"), aggregate2_1)
	aggregateBase2_2.SetAggregateVersion(1)

	tests := []struct {
		name      string
		fields    fields
		args      args
		preInsert []eh.Aggregate
		want      eh.Aggregate
		wantErr   bool
	}{
		// {
		// 	name: "restore aggreate with empty snapshot",
		// 	fields: fields{
		// 		session:        session,
		// 		boundedContext: "restore_snapshot",
		// 	},
		// 	args: args{
		// 		ctx: context.Background(),
		// 		aggregate: &aggregateTest{
		// 			AggregateBase: aggregateBase1,
		// 			data:          []string{"1", "2"},
		// 		},
		// 	},
		// 	want: &aggregateTest{
		// 		AggregateBase: aggregateBase1,
		// 		data:          []string{"1", "2"},
		// 	},
		// 	wantErr: false,
		// },
		{
			name: "restore aggreate",
			fields: fields{
				session:        session,
				boundedContext: "restore_snapshot",
			},
			preInsert: []eh.Aggregate{
				&aggregateTest{
					AggregateBase: aggregateBase2_1,
					data:          []string{"1", "2", "3"},
				},
			},
			args: args{
				ctx: context.Background(),
				aggregate: &aggregateTest{
					AggregateBase: aggregateBase2_2,
					data:          []string{"1", "2"},
				},
			},
			want: &aggregateTest{
				AggregateBase: aggregateBase2_1,
				data:          []string{"1", "2", "3"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &aggregateSnapshot{
				session:        tt.fields.session,
				boundedContext: tt.fields.boundedContext,
			}

			if len(tt.preInsert) > 0 {
				for _, agg := range tt.preInsert {
					err := a.Store(tt.args.ctx, agg)
					if err != nil {
						t.Error("pre_insert error", err)
					}
				}
			}

			got, err := a.Restore(tt.args.ctx, tt.args.aggregate)
			if (err != nil) != tt.wantErr {
				t.Errorf("aggregateSnapshot.Restore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got.AggregateType() != tt.want.AggregateType() {
				t.Error("aggregateSnapshot.Restore() error aggregate type")
				return
			}

			if got.EntityID() != tt.want.EntityID() {
				t.Error("aggregateSnapshot.Restore() error entity_id")
				return
			}

			aggregate, ok := got.(Aggregate)
			if !ok {
				t.Error("aggregateSnapshot.Restore() fail to implement interface")
				return
			}

			aggregateRestored, ok := tt.want.(Aggregate)
			if !ok {
				t.Error("aggregateSnapshot.Restore() fail to implement interface")
				return
			}

			if aggregate.AggregateVersion() != aggregateRestored.AggregateVersion() {
				t.Error("aggregateSnapshot.Restore() incorrect aggregate version")
				return
			}

			if !reflect.DeepEqual(aggregate.SnapshotData(), aggregateRestored.SnapshotData()) {
				t.Errorf("aggregateSnapshot.Restore() incorrect snapshot %v %v", aggregate.SnapshotData(), aggregateRestored.SnapshotData())
				return
			}
		})
	}
}
