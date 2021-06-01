package tracer

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sync"

	"go.opentelemetry.io/otel/trace"
)

type MyIdGenerator struct {
	TraceID trace.TraceID
	SpanID  trace.SpanID

	sync.Mutex
	randSource *rand.Rand
}

func (gen *MyIdGenerator) NewIDs(ctx context.Context) (tid trace.TraceID, sid trace.SpanID) {
	gen.randSource.Read(sid[:])

	if gen.TraceID == (trace.TraceID{}) {
		gen.randSource.Read(tid[:])
	} else {
		tid = gen.TraceID
	}
	if gen.SpanID == (trace.SpanID{}) {
		gen.randSource.Read(tid[:])
	} else {
		sid = gen.SpanID
	}
	return
}

func (gen *MyIdGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) (sid trace.SpanID) {
	gen.randSource.Read(sid[:])

	if gen.SpanID == (trace.SpanID{}) {
		gen.randSource.Read(sid[:])
	} else {
		sid = gen.SpanID
	}
	return
}

func NewMyIDGenerator() *MyIdGenerator {
	gen := MyIdGenerator{}

	var rngSeed int64
	_ = binary.Read(crand.Reader, binary.LittleEndian, &rngSeed)
	gen.randSource = rand.New(rand.NewSource(rngSeed))

	return &gen
}
