package receiver

import (
	"encoding/json"
	"log"
	"sync/atomic"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// logSampleRate is how often (1 in N successfully-decoded requests) each
// route below logs a full JSON dump of the decoded request body, purely as
// a human-readable example of the wire format each protocol actually sends
// - not for production observability.
const logSampleRate = 100

// sampleEvery increments counter and reports whether this call lands on a
// sampled request: the 1st, then every logSampleRate-th one after that, so
// each route logs an example immediately on startup rather than waiting
// logSampleRate requests for the first one.
func sampleEvery(counter *atomic.Uint64) (n uint64, sampled bool) {
	n = counter.Add(1)
	return n, (n-1)%logSampleRate == 0
}

// logSampleJSON logs v - a gogo/protobuf-generated message such as
// prompb.WriteRequest, which already carries its own encoding/json struct
// tags - as indented JSON.
func logSampleJSON(route string, n uint64, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Printf("%s: sample #%d: json.MarshalIndent err: %v", route, n, err)
		return
	}
	log.Printf("%s: sample request #%d, decoded:\n%s", route, n, b)
}

// logSampleProtoJSON logs m - a google.golang.org/protobuf message, such as
// the OTLP types - as indented JSON via protojson, which (unlike
// encoding/json, and unlike logSampleJSON's messages) understands
// protobuf's field naming and well-known types.
func logSampleProtoJSON(route string, n uint64, m proto.Message) {
	b, err := (protojson.MarshalOptions{Multiline: true, Indent: "  "}).Marshal(m)
	if err != nil {
		log.Printf("%s: sample #%d: protojson.Marshal err: %v", route, n, err)
		return
	}
	log.Printf("%s: sample request #%d, decoded:\n%s", route, n, b)
}
