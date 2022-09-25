package processor

import (
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

// MongoDBConfig contains configuration fields for the MongoDB processor.
type MongoDBConfig struct {
	MongoDB client.Config `json:",inline" yaml:",inline"`

	Operation       string                 `json:"operation" yaml:"operation"`
	FilterMap       string                 `json:"filter_map" yaml:"filter_map"`
	DocumentMap     string                 `json:"document_map" yaml:"document_map"`
	Upsert          bool                   `json:"upsert" yaml:"upsert"`
	Ordered         bool                   `json:"ordered" yaml:"ordered"`
	HintMap         string                 `json:"hint_map" yaml:"hint_map"`
	RetryConfig     retries.Config         `json:",inline" yaml:",inline"`
	JSONMarshalMode client.JSONMarshalMode `json:"json_marshal_mode" yaml:"json_marshal_mode"`
}

// NewMongoDBConfig returns a MongoDBConfig with default values.
func NewMongoDBConfig() MongoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return MongoDBConfig{
		MongoDB:         client.Config{},
		Operation:       "insert-one",
		Ordered:         true,
		RetryConfig:     rConf,
		JSONMarshalMode: client.JSONMarshalModeCanonical,
	}
}
