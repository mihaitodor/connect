package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func snowflakePutOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Summary("Performs batch operations on a MongoDB collection.").
		Description(output.Description(true, true, ""))

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec.Field(outputOperationDocs(client.OperationUpdateOne)).
		Field(writeConcernDocs()).
		Field(service.NewBloblangField("document_map").Description(
			"A bloblang map representing the records in the mongo db. Used to generate the document for mongodb by mapping the fields in the message to the mongodb fields. The document map is required for the `insert-one`, `replace-one` and `update-one` operations.",
		).Example("root.a = this.foo\nroot.b = this.bar").Optional()).
		Field(service.NewBloblangField("filter_map").Description(
			"A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except `insert-one`. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should have the fields required to locate the document to delete.",
		).Example("root.a = this.foo\nroot.b = this.bar").Optional()).
		Field(service.NewBloblangField("hint_map").Description(
			"A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations except `insert-one`. It is used to improve performance of finding the documents in the mongodb.",
		).Example("root.a = this.foo\nroot.b = this.bar").Optional()).
		Field(service.NewBoolField("upsert").Description("The upsert setting is optional and only applies for `update-one` and `replace-one` operations. If the filter specified in filter_map matches, the document is updated or replaced accordingly, otherwise it is created.").Default(false).Version("3.60.0")).
		Field(service.NewBoolField("ordered").Description("A boolean specifying whether the mongod instance should perform an ordered or unordered bulk operation execution. If the order doesn't matter, setting this to false can increase write performance.").Default(true).Advanced().Version("4.6.1")).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewIntField("max_in_flight").Description("The maximum number of parallel message batches to have in flight at any given time.").Default(64))

	spec.Version("3.43.0")
	// spec.Example() // TODO: document_map - update-one (and maybe replace-one?!?) needs an operator (i.e. `$set`) https://github.com/benthosdev/benthos/issues/1265

	return spec
}

func init() {
	err := service.RegisterBatchOutput("mongodb", snowflakePutOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newMongoDBOutputFromConfig(conf, mgr.Logger(), mgr.Metrics())
			return
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type mongoDBOutput struct {
	operation    client.Operation
	database     string
	collection   *service.InterpolatedString
	writeConcern *options.CollectionOptions
	documentMap  *bloblang.Executor
	filterMap    *bloblang.Executor
	hintMap      *bloblang.Executor
	upsert       bool
	ordered      bool

	connMut sync.RWMutex
	client  *mongo.Client
	db      *mongo.Database

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func newMongoDBOutputFromConfig(conf *service.ParsedConfig, logger *service.Logger, metrics *service.Metrics) (*mongoDBOutput, error) {
	m := mongoDBOutput{
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
	}

	var err error

	clientConf := client.Config{}

	if clientConf.URL, err = conf.FieldString("url"); err != nil {
		return nil, err
	}

	if clientConf.Username, err = conf.FieldString("username"); err != nil {
		return nil, err
	}

	if clientConf.Password, err = conf.FieldString("password"); err != nil {
		return nil, err
	}

	if m.database, err = conf.FieldString("database"); err != nil {
		return nil, err
	}

	if m.collection, err = conf.FieldInterpolatedString("collection"); err != nil {
		return nil, err
	}

	if op, err := conf.FieldString("operation"); err != nil {
		return nil, err
	} else {
		m.operation = client.NewOperation(op)
	}

	if conf.Contains("write_concern") {
		var wcOpts []writeconcern.Option

		wcConf := conf.Namespace("write_concern")

		var w string
		if w, err = wcConf.FieldString("w"); err != nil {
			return nil, err
		}

		if instances, err := strconv.Atoi(w); err != nil {
			wcOpts = append(wcOpts, writeconcern.WTagSet(w))
		} else {
			wcOpts = append(wcOpts, writeconcern.W(instances))
		}

		var j bool
		if j, err = wcConf.FieldBool("j"); err != nil {
			return nil, err
		}
		wcOpts = append(wcOpts, writeconcern.J(j))

		var timeout time.Duration
		if timeout, err = wcConf.FieldDuration("w_timeout"); err != nil {
			return nil, err
		}
		wcOpts = append(wcOpts, writeconcern.WTimeout(timeout))

		wc := writeconcern.New(wcOpts...)

		if !wc.IsValid() {
			return nil, fmt.Errorf("write_concern validation error: %s", err)
		}

		m.writeConcern = options.Collection().SetWriteConcern(wc)
	}

	if conf.Contains("document_map") {
		if !isDocumentAllowed(m.operation) {
			return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", m.operation)
		} else if m.documentMap, err = conf.FieldBloblang("document_map"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("filter_map") {
		if !isFilterAllowed(m.operation) {
			return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", m.operation)
		} else if m.filterMap, err = conf.FieldBloblang("filter_map"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("hint_map") {
		if !isHintAllowed(m.operation) {
			return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", m.operation)
		} else if m.hintMap, err = conf.FieldBloblang("hint_map"); err != nil {
			return nil, err
		}
	}

	if m.upsert, err = conf.FieldBool("upsert"); err != nil {
		return nil, err
	}

	if !isUpsertAllowed(m.operation) && m.upsert {
		return nil, fmt.Errorf("mongodb upsert not allowed for '%s' operation", m.operation)
	}

	if m.ordered, err = conf.FieldBool("ordered"); err != nil {
		return nil, err
	}

	if m.client, err = clientConf.Client(); err != nil {
		return nil, err
	}

	return &m, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the target mongo DB
func (m *mongoDBOutput) Connect(ctx context.Context) error {
	m.connMut.Lock()
	defer m.connMut.Unlock()

	if m.db != nil {
		return nil
	}

	if err := m.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	if err := m.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}

	m.db = m.client.Database(m.database)

	go func() {
		<-m.shutSig.CloseNowChan()

		m.connMut.Lock()
		_ = m.client.Disconnect(ctx)
		m.connMut.Unlock()

		m.shutSig.ShutdownComplete()
	}()

	return nil
}

// WriteBatch attempts to perform the designated operation to the mongo DB collection.
func (m *mongoDBOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	m.connMut.RLock()
	defer m.connMut.RUnlock()

	writeModelsMap := map[*mongo.Collection][]mongo.WriteModel{}
	for i, msg := range batch {
		var err error
		var filterVal, documentVal *service.Message
		var upsertVal, filterValWanted, documentValWanted bool

		filterValWanted = isFilterAllowed(m.operation)
		documentValWanted = isDocumentAllowed(m.operation)

		if filterValWanted {
			if filterVal, err = batch.BloblangQuery(i, m.filterMap); err != nil {
				return fmt.Errorf("failed to execute filter_map: %s", err)
			}
		}

		if (filterVal != nil || !filterValWanted) && documentValWanted {
			if documentVal, err = batch.BloblangQuery(i, m.documentMap); err != nil {
				return fmt.Errorf("failed to execute document_map: %s", err)
			}
		}

		if filterVal == nil && filterValWanted {
			return errors.New("failed to generate filterVal")
		}

		if documentVal == nil && documentValWanted {
			return errors.New("failed to generate documentVal")
		}

		var docJSON, filterJSON, hintJSON interface{}

		if filterValWanted {
			if filterJSON, err = filterVal.AsStructured(); err != nil {
				return err
			}
		}

		if documentValWanted {
			if docJSON, err = documentVal.AsStructured(); err != nil {
				return err
			}
		}

		if m.hintMap != nil {
			if hintVal, err := batch.BloblangQuery(i, m.hintMap); err != nil {
				return fmt.Errorf("failed to execute hint_map: %s", err)
			} else if hintJSON, err = hintVal.AsStructured(); err != nil {
				return err
			}
		}

		var writeModel mongo.WriteModel
		collection := m.db.Collection(m.collection.String(msg), m.writeConcern)

		switch m.operation {
		case client.OperationInsertOne:
			writeModel = &mongo.InsertOneModel{
				Document: docJSON,
			}
		case client.OperationDeleteOne:
			writeModel = &mongo.DeleteOneModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case client.OperationDeleteMany:
			writeModel = &mongo.DeleteManyModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case client.OperationReplaceOne:
			writeModel = &mongo.ReplaceOneModel{
				Upsert:      &upsertVal,
				Filter:      filterJSON,
				Replacement: docJSON,
				Hint:        hintJSON,
			}
		case client.OperationUpdateOne:
			writeModel = &mongo.UpdateOneModel{
				Upsert: &upsertVal,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		}

		if writeModel != nil {
			writeModelsMap[collection] = append(writeModelsMap[collection], writeModel)
		}
	}

	// Dispatch any documents which IterateBatchedSend managed to process successfully
	if len(writeModelsMap) > 0 {
		opts := options.BulkWrite().SetOrdered(m.ordered)
		for collection, writeModels := range writeModelsMap {
			if _, err := collection.BulkWrite(context.Background(), writeModels, opts); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close begins cleaning up resources used by this writer.
func (m *mongoDBOutput) Close(ctx context.Context) error {
	m.shutSig.CloseNow()
	m.connMut.Lock()
	isNil := m.client == nil
	m.connMut.Unlock()
	if isNil {
		return nil
	}
	select {
	case <-m.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
