package tracer

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

const (
	AuditTraceName = "jaeger-audit.woqutech.traceid"
	AuditSelfName = "jaeger-audit.woqutech.selfid"   // 直接使用k8s资源的uuid. 由于第一次没有uuid，所以需要保留

	TraceLabelName = "AppName"
)

func EncodeUuid(uid uuid.UUID) string  {
	return hex.EncodeToString(uid[:])
}

func K8sUidToSpanId(str string) (trace.SpanID, error)  {
	if str == "" {
		return trace.SpanID{}, fmt.Errorf("get empty uid. ")
	}
	str = strings.ReplaceAll(str, "-", "")
	return trace.SpanIDFromHex(str[:16]) // 只取前16位
}

func GetSpanIdBYTraceId(tid trace.TraceID) trace.SpanID  {
	sid := trace.SpanID{}
	for i:=0;i<len(sid);i++{
		sid[i] = tid[i]
	}
	return sid
}
