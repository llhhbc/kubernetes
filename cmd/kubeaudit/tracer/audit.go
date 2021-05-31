package tracer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mattbaird/jsonpatch"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"k8s.io/api/admission/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

)

type JaegerAudit struct {
	workChain chan Msg
	sync.Mutex
	mid *MyIdGenerator

	Flush func()
	l     *zap.Logger
}

type Msg struct {
	Request *v1beta1.AdmissionRequest
	Object *unstructured.Unstructured
	FirstSelfId bool
}

func tracerProvider(url, serverName string) (*sdktrace.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.NewRawExporter(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// Record information about this application in an Resource.
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.ServiceNameKey.String(serverName),
		)),
	)
	return tp, nil
}

// must defer Flush()
func NewJaegerAudit(jaegerServer, serverName string, l *zap.Logger, workers int) (*JaegerAudit, error) {
	res := JaegerAudit{}

	if workers <= 0 {
		workers = 10
	}

	res.workChain = make(chan Msg, workers*10)
	res.l = l
	res.mid = NewMyIDGenerator()

	tp, err := tracerProvider(jaegerServer, serverName)
	if err != nil {
		return nil, fmt.Errorf("new jaeger exporter failed %v. ", err)
	}
	otel.SetTracerProvider(tp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}(ctx)


	for i := 0; i < workers; i++ {
		go res.runWorker()
	}

	return &res, nil
}

func (t *JaegerAudit) Send(obj Msg) {
	select {
	case t.workChain <- obj:
		return
	case <-time.After(time.Second):
		t.l.Error("send msg to workChain timeout. ")
		return
	}
}

/*
jaeger-ui显示方式：
左边查询：
1. servie  对应 创建 NewJaegerAudit时的 serverName
2. operation 对应 otel.Tracer(name) 名称

右边查询结果：
1. 相同parentId的记录，会合并显示。
2. 如果selfId对应有相关的parentId的记录，可支持跳转显示

审计特点：
1. create事件时，无资源uid，会生成traceID
2. 根据owner可生成parentID
3. 根据uid可生成selfID

所以：
1. create事件时，生成一个span：  traceID, traceID[:8]
2. 后续该资源的所有操作，parentID统一为： traceID[:8], selfID自动生成
3. 如果资源有父资源，需要在创建时，创建的span为： traceID, 父资源id

 */

const (
	spanJob = "spanJobName"
)
func (t *JaegerAudit) runWorker() {
	for {
		msg := <-t.workChain

		t.DoJaegerObject(msg)
	}
}

func (t *JaegerAudit) DoJaegerObject(msg Msg)  {
	obj := msg.Object

	mapInfo := obj.GetAnnotations()
	name := obj.GetName()
	uid := string(obj.GetUID())

	if mapInfo == nil {
		return // skip
	}

	curl := t.l.With(zap.String("uuid", uid),
		zap.String("traceId", mapInfo[AuditTraceName]))

	var err error
	var selfId, parentId trace.SpanID

	selfId, err = K8sUidToSpanId(uid)
	if err != nil && (msg.FirstSelfId || msg.Request.Operation != v1beta1.Create) { // 这两种情况会用到selfId
		curl.Error("parse self id failed. ", zap.Error(err))
		return
	}

	traceID, err := trace.TraceIDFromHex(mapInfo[AuditTraceName])
	if err != nil {
		curl.Error("parse trace id failed. ", zap.Error(err))
		return
	}

	owner := obj.GetOwnerReferences()
	parentUid := ""
	subResource := false // 标识是否是子资源
	if owner != nil && len(owner) > 0 {
		subResource = true
		parentUid = string(owner[0].UID)
	} else {
		parentUid = mapInfo[AuditTraceName]
	}
	parentId, err = K8sUidToSpanId(parentUid)
	if err != nil {
		curl.Error("parse parent id failed. ", zap.Error(err))
		return
	}

	ctx := context.Background()

	tr := otel.Tracer(name)
	var span trace.Span

	parentCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     parentId,
		TraceFlags: 0,
		TraceState: trace.TraceState{},
		Remote:     false,
	})

	if msg.Request.Operation == v1beta1.Create {
		// 需要多生成一个基础span
		t.Lock()
		t.mid.TraceID = traceID
		if !subResource {
			t.mid.SpanID = parentId // traceId[:8]
			_, span = tr.Start(ctx, name)
		} else {
			t.mid.SpanID = trace.SpanID{}
			_, span = tr.Start(trace.ContextWithRemoteSpanContext(ctx, parentCtx), name)
		}
		t.Unlock()
		defer span.End()
		span.SetAttributes(attribute.String(spanJob, name+"-rootCreate"))
		curl.Info("gen root span: ",
			zap.String("spanId", span.SpanContext().SpanID().String()),
			zap.Bool("subResource", subResource),
			zap.String("parentId", parentCtx.SpanID().String()),
			)
	} else if msg.FirstSelfId {
		// 生成parentID与selfID的关系
		t.Lock()
		t.mid.TraceID = traceID
		t.mid.SpanID = selfId
		_, span = tr.Start(trace.ContextWithRemoteSpanContext(ctx, parentCtx), name)
		defer span.End()
		t.Unlock()
		span.SetAttributes(attribute.String(spanJob, name+"-self"))
		curl.Info("gen self span: ",
			zap.String("spanId", span.SpanContext().SpanID().String()),
			zap.String("parentId", parentCtx.SpanID().String()))
	} else {
		parentCtx = parentCtx.WithSpanID(selfId)
		t.Lock()
		t.mid.TraceID = traceID
		t.mid.SpanID = trace.SpanID{}
		ctx, span = tr.Start(trace.ContextWithRemoteSpanContext(ctx, parentCtx), name)
		t.Unlock()
		span.SetAttributes(attribute.String(spanJob,
			fmt.Sprintf("%s-%s-%s-%s", name, msg.Request.Operation, msg.Request.SubResource, time.Now().Format(time.RFC3339))))
		curl.Info("gen operator span: ",
			zap.String("spanId", span.SpanContext().SpanID().String()),
			zap.String("parentId", parentCtx.SpanID().String()))
		defer span.End()
	}


	span.SetAttributes(
		attribute.String("traceId", traceID.String()),
		attribute.String("spanId", selfId.String()),
		attribute.String("parentId", parentId.String()),
	)

	//req, _ := json.Marshal(msg.Request)
	span.SetAttributes(
		attribute.String("uid", uid),
		attribute.String("parentUid", parentUid),
		attribute.String("operator", string(msg.Request.Operation)),
		//attribute.String("requstDump", string(req)),
	)

	if msg.Request.Operation == v1beta1.Update { // 只有update时，需要获取变更信息
		m, _ := jsonpatch.CreatePatch(msg.Request.OldObject.Raw, msg.Request.Object.Raw)
		ms, _ := json.Marshal(m)
		span.SetAttributes(attribute.String("updateDiff", string(ms)))
	}
}