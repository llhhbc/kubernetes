package tracer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/klog"

	"k8s.io/kubernetes/cmd/kubeaudit/db"
)

/*
根据数据库中保存的审计信息，生成trace信息

1. 根资源的版本变更，会触发生成traceID，uid为k8s的uid保持不变。三元组为：traceid, uid, <nil>
2. 子资源
     根据子资源的parent_uuid与event_time,找到与该时间最接近的，并且比它小的，
     有trace_id或者parent_uuid的记录，如果找到trace_id，则停止，否则继续找上层
   如果缓存中traceid与找到的相同（表示是更新事件）：
     生成的三元组为：traceid, uid, <spanID>
   否则（表示新生成的traceID）：
     生成的三元组为： traceid, parent_uid, uid
     之后更新缓存

*/

type JaegerAudit struct {
	mid *MyIdGenerator

	groupName string

	traceInfo map[string]string // key is uid, valud is traceid
	traceLock sync.RWMutex

	tp trace.TracerProvider
}

func tracerProvider(url, serverName string, g sdktrace.IDGenerator) (*sdktrace.TracerProvider, error) {
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
		sdktrace.WithIDGenerator(g),
	)
	return tp, nil
}

// must defer Flush()
func RunJaegerAudit(jaegerServer, serverName, groupName string) {
	res := JaegerAudit{}
	res.mid = NewMyIDGenerator()
	res.traceInfo = make(map[string]string, 0)
	res.groupName = groupName

	if groupName != "" {
		serverName = groupName
	}
	tp, err := tracerProvider(jaegerServer, groupName, res.mid)
	if err != nil {
		klog.Fatalf("new jaeger exporter failed %v. ", err)
	}
	if groupName != "" {
		otel.SetTracerProvider(tp)
	}
	res.tp = tp

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			klog.Fatal(err)
		}
	}(ctx)

	res.Run()
}

const (
	spanJob = "spanJobName"
)

func (t *JaegerAudit) Run() {
	startTime := time.Now().UnixNano()
	//klog.Info("start jaeger from %d. ", startTime)

	ldb := db.DB
	for {
		time.Sleep(20 * time.Second)
		klog.Infof("%s scan start time %s. ", t.groupName, time.Unix(startTime/1e9, startTime%1e9).Format(time.RFC3339Nano))
		cdb := ldb.Model(&db.AuditInfo{}).Where("event_time > ? ", startTime)
		if t.groupName != "" {
			cdb.Where("uuid in (select uuid from meta_data where api_version = ? ) ", t.groupName)
		}
		row, err := cdb.Rows()
		if err != nil {
			klog.Errorf("get audit info failed %v. ", err)
			continue
		}
		scanStart := time.Now()
		count := 0
		for row.Next() {
			count ++
			ai := &db.AuditInfo{}
			err = ldb.ScanRows(row, &ai)
			if err != nil {
				klog.Errorf("scam row failed %v. ", err)
				break
			}
			md := &db.MetaData{}
			err = ldb.Find(md, "uuid = ? ", ai.Uuid).Error
			if err != nil {
				klog.Errorf("get metadata by uuid failed %v. ", err)
				break
			}
			if t.groupName != "" && md.ApiVersion != t.groupName {
				continue
			}
			if ai.SpanID != "" {
				klog.Errorf("[%s,%s,%s] ai's spanid is not empty, this can't be happened. ",
					ai.Uuid, ai.ResVersion, ai.SpanID)
				continue
			}
			t.DoAuditInfo(ai, md)
			startTime = ai.EventTime // increase
		}
		row.Close()
		klog.Infof("%s scan end, start %s, end %s count %d. ", t.groupName,
			scanStart.Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano), count)
	}
}

/*
workFlow:
1 if is root:  traceID, uuid, <spanID>   ---> end
2.2 get parentID and traceID -- Recursively get the closest event_time's record
 3. check uuid->traceID cache
 3.1(No) : traceID, parentID, selfID
   4.1:  update cache: uuid->traceID  ---> end
 3.2(Yes): traceID, selfID, <spanID>
*/

/*
增加二套显示方式：
traceID统一为根资源的uid，这样单一资源的修改就整合了
 */
func (t *JaegerAudit) DoAuditInfo(ai *db.AuditInfo, md *db.MetaData) {
	if ai.IsRoot {
		t.JaegerRecord(ai.TraceId, ai.Uuid, "", ai, md)
		return
	}
	parentAI, err := GetParentAI(ai)
	if err != nil {
		klog.Errorf("get parent info failed %v. ", err)
		return
	}
	t.traceLock.RLock()
	tid := t.traceInfo[ai.Uuid]
	t.traceLock.RUnlock()

	if tid != "" && tid == parentAI.TraceId { // 3.2
		t.JaegerRecord(parentAI.TraceId, ai.Uuid, "", ai, md)
	} else { // 3.1
		t.JaegerRecord(parentAI.TraceId, ai.ParentUuid, ai.Uuid, ai, md)
		t.traceLock.Lock()
		t.traceInfo[ai.Uuid] = parentAI.TraceId
		t.traceLock.Unlock()
	}
}

func (t *JaegerAudit) JaegerRecord(traceID, parentID, spanID string, ai *db.AuditInfo, md *db.MetaData) {
	if traceID == "" || parentID == "" {
		klog.Errorf("get empty traceID %s or parentID %s, %s. ", traceID, parentID, ai.ToString())
		return
	}
	var pid, sid trace.SpanID
	hasParent := true
	tid, err := trace.TraceIDFromHex(strings.ReplaceAll(traceID, "-", ""))
	if err != nil {
		klog.Errorf("get invalid traceID %s %s. ", traceID, err)
		return
	}
	pid, err = K8sUidToSpanId(parentID)
	if err != nil {
		klog.Errorf("get invalid parentID %s %s. ", parentID, err)
		return
	}
	if spanID == "" {
		sid = trace.SpanID{}
	} else {
		hasParent = false
		sid, err = K8sUidToSpanId(spanID)
		if err != nil {
			klog.Errorf("get invalid spanID %s %s. ", spanID, err)
			return
		}
	}

	ctx := context.Background()
	tr := t.tp.Tracer(md.SelfLink)

	spanName := fmt.Sprintf("%s/%s/%s", md.Kind, md.Namespace, md.Name)

	var span trace.Span
	if !hasParent {
		t.mid.Lock()
		t.mid.TraceID = tid
		t.mid.SpanID = pid
		_, span = tr.Start(ctx, spanName)
		defer span.End()
		t.mid.Unlock()

		span.SetAttributes(attribute.String(spanJob, "new-version-created"))
	} else {
		parentCtx := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     pid,
			TraceFlags: 0,
			TraceState: trace.TraceState{},
			Remote:     false,
		})
		t.mid.Lock()
		t.mid.TraceID = tid
		t.mid.SpanID = sid
		_, span = tr.Start(trace.ContextWithRemoteSpanContext(ctx, parentCtx), spanName)
		defer span.End()
		t.mid.Unlock()
		span.SetAttributes(attribute.String(spanJob, ""))
	}
	span.SetAttributes(
		attribute.String("spanInfo", fmt.Sprintf("tid: %s, pid: %s, sid: %s. ",
			span.SpanContext().TraceID().String(),
			parentID,
			span.SpanContext().SpanID().String())),
		)
	span.SetAttributes(
		attribute.String("uid", ai.Uuid),
		attribute.String("api_version", md.ApiVersion),
		attribute.String("kind", md.Kind),
		attribute.String("name", md.Name),
		attribute.String("namespace", md.Namespace),
		//attribute.String("context", ai.Context),
		attribute.String("event_time", time.Unix(ai.EventTime/1e9, ai.EventTime%1e9).Format(time.RFC3339Nano)),
		attribute.String("context_diff", ai.ContextDiff),
		attribute.String("parent_uuid", ai.ParentUuid),
		attribute.String("res_version", ai.ResVersion),
		attribute.String("old_version", ai.OldVersion),
	)
	err = db.DB.Model(&db.AuditInfo{}).Where("uuid = ? and res_version = ? ",
		ai.Uuid, ai.ResVersion).Updates(&db.AuditInfo{
		SpanID: span.SpanContext().SpanID().String(),
	}).Error
	if err != nil {
		klog.Errorf("record span info [%s,%s,%s] failed %v. ",
			ai.Uuid, ai.ResVersion, span.SpanContext().SpanID().String(),err)
	}
}

func GetParentAI(ai *db.AuditInfo) (*db.AuditInfo, error) {
	if ai.IsRoot {
		return ai, nil
	}
	pai := &db.AuditInfo{}
	err := db.DB.Limit(1).Model(&pai).Where("uuid = ? and event_time <= ? ",
		ai.ParentUuid, ai.EventTime).Order("event_time DESC").Scan(&pai).Error
	if err != nil {
		return nil, fmt.Errorf("get parent uuid by %s failed %v. ", ai.ParentUuid, err)
	}
	if pai.IsRoot {
		return pai, nil
	}
	return GetParentAI(pai)
}
