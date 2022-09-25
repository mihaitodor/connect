package client

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Operation represents the operation that will be performed by MongoDB.
type Operation string

const (
	// OperationInsertOne Insert One operation.
	OperationInsertOne Operation = "insert-one"
	// OperationDeleteOne Delete One operation.
	OperationDeleteOne Operation = "delete-one"
	// OperationDeleteMany Delete many operation.
	OperationDeleteMany Operation = "delete-many"
	// OperationReplaceOne Replace one operation.
	OperationReplaceOne Operation = "replace-one"
	// OperationUpdateOne Update one operation.
	OperationUpdateOne Operation = "update-one"
	// OperationFindOne Find one operation.
	OperationFindOne Operation = "find-one"
	// OperationInvalid Invalid operation.
	OperationInvalid Operation = "invalid"
)

// NewOperation converts a string operation to a strongly-typed Operation.
func NewOperation(op string) Operation {
	switch op {
	case "insert-one":
		return OperationInsertOne
	case "delete-one":
		return OperationDeleteOne
	case "delete-many":
		return OperationDeleteMany
	case "replace-one":
		return OperationReplaceOne
	case "update-one":
		return OperationUpdateOne
	case "find-one":
		return OperationFindOne
	default:
		return OperationInvalid
	}
}

// JSONMarshalMode represents the way in which BSON should be marshalled to JSON.
type JSONMarshalMode string

const (
	// JSONMarshalModeCanonical Canonical BSON to JSON marshal mode.
	JSONMarshalModeCanonical JSONMarshalMode = "canonical"
	// JSONMarshalModeRelaxed Relaxed BSON to JSON marshal mode.
	JSONMarshalModeRelaxed JSONMarshalMode = "relaxed"
)

// Config is a config struct for a mongo connection.
type Config struct {
	URL      string
	Username string
	Password string
}

// WriteConcern describes a write concern for MongoDB.
type WriteConcern1 struct {
	W        string
	J        bool
	WTimeout string
}

// Client returns a new mongodb client based on the configuration parameters.
func (m Config) Client() (*mongo.Client, error) {
	opt := options.Client().
		SetConnectTimeout(10 * time.Second).
		SetSocketTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		ApplyURI(m.URL)

	if m.Username != "" && m.Password != "" {
		creds := options.Credential{
			Username: m.Username,
			Password: m.Password,
		}
		opt.SetAuth(creds)
	}

	client, err := mongo.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}

	return client, nil
}
