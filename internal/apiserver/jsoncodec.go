package apiserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// snakeCaseJSONCodec replaces Connect-go's built-in "json" codec, which
// marshals using protojson's default options - protobuf's official JSON
// mapping (https://protobuf.dev/programming-guides/json/) converts every
// snake_case proto field name to camelCase in JSON by default (e.g.
// external_account_ref -> externalAccountRef), which is what every
// gen/openapi/*.yaml field name reflected until this codec was added.
// This project's own .proto field names are snake_case and that's the
// wire convention wanted here too, so this codec sets UseProtoNames to
// keep JSON field names identical to the .proto source instead. Register
// it under both connect's "json" and "json; charset=utf-8" codec names
// (option.go's own WithProtoJSON registers the default under both) or
// only one transport's requests get the override.
type snakeCaseJSONCodec struct {
	name string
}

var _ connect.Codec = (*snakeCaseJSONCodec)(nil)

// NewSnakeCaseJSONCodecs returns the connect.HandlerOptions that
// override Connect-go's default "json"/"json; charset=utf-8" codecs
// with snakeCaseJSONCodec - wire into every service handler's shared
// options (cmd/coreapi/main.go's handlerOpts) so JSON field names match
// this project's .proto field names instead of camelCase.
func NewSnakeCaseJSONCodecs() []connect.HandlerOption {
	return []connect.HandlerOption{
		connect.WithCodec(&snakeCaseJSONCodec{name: "json"}),
		connect.WithCodec(&snakeCaseJSONCodec{name: "json; charset=utf-8"}),
	}
}

func (c *snakeCaseJSONCodec) Name() string { return c.name }

func (c *snakeCaseJSONCodec) Marshal(message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("apiserver: message of type %T does not implement proto.Message", message)
	}
	return protojson.MarshalOptions{UseProtoNames: true}.Marshal(protoMessage)
}

func (c *snakeCaseJSONCodec) MarshalAppend(dst []byte, message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("apiserver: message of type %T does not implement proto.Message", message)
	}
	return protojson.MarshalOptions{UseProtoNames: true}.MarshalAppend(dst, protoMessage)
}

func (c *snakeCaseJSONCodec) MarshalStable(message any) ([]byte, error) {
	messageJSON, err := c.Marshal(message)
	if err != nil {
		return nil, err
	}
	compactedJSON := bytes.NewBuffer(messageJSON[:0])
	if err := json.Compact(compactedJSON, messageJSON); err != nil {
		return nil, err
	}
	return compactedJSON.Bytes(), nil
}

func (c *snakeCaseJSONCodec) Unmarshal(binary []byte, message any) error {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("apiserver: message of type %T does not implement proto.Message", message)
	}
	if len(binary) == 0 {
		return errors.New("apiserver: zero-length payload is not a valid JSON object")
	}
	// protojson's unmarshaler already accepts both snake_case and
	// camelCase field names regardless of UseProtoNames (that option only
	// affects Marshal) - DiscardUnknown matches connect-go's own default
	// codec so schema-evolution tolerance doesn't regress.
	options := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := options.Unmarshal(binary, protoMessage); err != nil {
		return fmt.Errorf("apiserver: unmarshal into %T: %w", message, err)
	}
	return nil
}

func (c *snakeCaseJSONCodec) IsBinary() bool { return false }
