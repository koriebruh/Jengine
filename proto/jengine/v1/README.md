# proto/jengine/v1

Contract-first API/event schema (`.proto`-first via Connect-RPC), populated starting `plans/task/core/10` (matching engine event schema) and `plans/task/core/15` (API layer). See `plans/docs/06-streaming-architecture.md` §7.2 and `plans/docs/07-api-extensibility.md` §8.1.

## Regenerating stubs (`make proto-gen`)

`buf generate` (wrapped by `make proto-gen`) produces:
- `gen/go/` — Go structs + Connect-RPC service/client code (`protoc-gen-go` + `protoc-gen-connect-go`).
- `gen/openapi/openapi.yaml` — a Swagger/OpenAPI 3.1 spec of the actual Connect-RPC surface (`protoc-gen-connect-openapi`, not google/gnostic's generic `protoc-gen-openapi` - these services have no `google.api.http` annotations, so gnostic's plugin emits an empty spec; connect-openapi reflects Connect's real default routing, `POST /<package>.<Service>/<Method>`, with no annotations needed).

Both are checked into git (not regenerated in CI) - install the plugins once, then re-run `make proto-gen` after editing any `.proto` file:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
go install github.com/sudorandom/protoc-gen-connect-openapi@latest
```
