package kafkasource_test

import (
	"testing"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/kafkasource"
)

func TestValidate(t *testing.T) {
	base := kafkasource.Config{
		BootstrapServers: []string{"broker:9092"},
		Topic:            "t",
		ConsumerGroup:    "g",
		AuthMode:         kafkasource.AuthModePlaintext,
		SourceFormat:     "JSON",
	}

	tests := []struct {
		name    string
		mutate  func(c *kafkasource.Config)
		wantErr bool
	}{
		{"valid plaintext", func(c *kafkasource.Config) {}, false},
		{"missing bootstrap_servers", func(c *kafkasource.Config) { c.BootstrapServers = nil }, true},
		{"missing topic", func(c *kafkasource.Config) { c.Topic = "" }, true},
		{"missing consumer_group", func(c *kafkasource.Config) { c.ConsumerGroup = "" }, true},
		{"missing source_format", func(c *kafkasource.Config) { c.SourceFormat = "" }, true},
		{"unsupported auth_mode", func(c *kafkasource.Config) { c.AuthMode = "made-up" }, true},
		{"sasl_ssl missing creds", func(c *kafkasource.Config) { c.AuthMode = kafkasource.AuthModeSASLSSL }, true},
		{"sasl_ssl with creds", func(c *kafkasource.Config) {
			c.AuthMode = kafkasource.AuthModeSASLSSL
			c.SASLUsernameRef = "secret/u"
			c.SASLPasswordRef = "secret/p"
		}, false},
		{"mtls missing cert/key", func(c *kafkasource.Config) { c.AuthMode = kafkasource.AuthModeMTLS }, true},
		{"mtls with cert/key", func(c *kafkasource.Config) {
			c.AuthMode = kafkasource.AuthModeMTLS
			c.TLSCertRef = "secret/cert"
			c.TLSKeyRef = "secret/key"
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			c := kafkasource.New(noopSecrets{})
			err := c.Validate(connector.ConnectorConfig{Settings: mustJSON(t, cfg)})
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
