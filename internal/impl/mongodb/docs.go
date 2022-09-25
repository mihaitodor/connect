package mongodb

import (
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

var defaultOutputOperations = []string{
	string(client.OperationInsertOne),
	string(client.OperationDeleteOne),
	string(client.OperationDeleteMany),
	string(client.OperationReplaceOne),
	string(client.OperationUpdateOne),
}

func processorOperationDocs(defaultOperation client.Operation) *service.ConfigField {
	return service.NewStringEnumField(
		"operation",
		append(defaultOutputOperations, string(client.OperationFindOne))...,
	).Description("The mongodb operation to perform.").Default(defaultOperation)
}

func outputOperationDocs(defaultOperation client.Operation) *service.ConfigField {
	return service.NewStringEnumField(
		"operation",
		defaultOutputOperations...,
	).Description("The mongodb operation to perform.").Default(defaultOperation)
}

func writeConcernDocs() *service.ConfigField {
	return service.NewObjectField("write_concern",
		service.NewStringField("w").Description("W requests acknowledgement that write operations propagate to the specified number of mongodb instances or to mongod instances with specified tags."),
		service.NewBoolField("j").Description("J requests acknowledgement from MongoDB that write operations are written to the journal."),
		service.NewDurationField("w_timeout").Description("The write concern timeout."),
	).Description("The write concern settings for the mongo connection.").Optional()
}

var urlField = service.NewStringField("url").
	Description("The URL of the target MongoDB DB.").
	Example("mongodb://localhost:27017")

var queryField = service.NewBloblangField("query").Description("Bloblang expression describing MongoDB query.").
	Example(`root.from = {"$lte": timestamp_unix()}
root.to = {"$gte": timestamp_unix()}`)
