package main

import (
	"encoding/json"
	"flag"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"github.com/google/uuid"
	"k8s.io/klog"
)

var DB *gorm.DB

/*
1. uuid 统一使用k8s的uuid, 并增加版本信息说明. uuid加版本为唯一键值
2. 当根资源发生变化时，会生成新的traceID
3. 子资源将继承父资源的traceID
 */
type AuditInfo struct {
	Uuid string `gorm:"type:varchar(36);primarykey"`
	ResVersion string `gorm:"type:varchar(20);primarykey"`
	Event string `gorm:"type:varchar(10)"`
	TraceId string `gorm:"type:varchar(36)"`
	IsRoot bool
	ParentUuid string `gorm:"type:varchar(36)"`
	Context []byte `gorm:"type:text(65535)"`
}

func (t *AuditInfo) ToString() string  {
	m, _ := json.Marshal(t)
	return string(m)
}

func NewAuditInfo(obj *unstructured.Unstructured, event string)  *AuditInfo {
	res := AuditInfo{}

	res.Uuid = string(obj.GetUID())
	res.ResVersion = obj.GetResourceVersion()
	res.Event = event

	owner := obj.GetOwnerReferences()
	if owner == nil || len(owner) == 0 {
		res.IsRoot = true
		res.TraceId = uuid.NewString()
	} else {
		res.ParentUuid = string(owner[0].UID)
	}
	res.Context, _ = obj.MarshalJSON()
	return &res
}

type MetaData struct {
	Uuid string `gorm:"type:varchar(36);primarykey"`
	SelfLink string `gorm:"type:varchar(200)"`
	ApiVersion string `gorm:"type:varchar(100)"`
	Kind string `gorm:"type:varchar(30)"`
	Name string `gorm:"type:varchar(100)"`
	Namespace string `gorm:"type:varchar(30)"`
	Annotations string `gorm:"type:varchar(1024)"`
	CreateTime  time.Time
	Labels string `gorm:"type:varchar(1024)"`
}

func NewMetaData(obj *unstructured.Unstructured)  *MetaData {
	res := MetaData{}

	res.Uuid = string(obj.GetUID())
	res.SelfLink = obj.GetSelfLink()
	res.ApiVersion = obj.GetAPIVersion()
	res.Kind = obj.GetKind()
	res.Name = obj.GetName()
	res.Namespace = obj.GetNamespace()
	//res.Annotations = obj.GetAnnotations()
	res.CreateTime = obj.GetCreationTimestamp().Time

	return &res
}

var (
	dbaddr = flag.String("dbaddr", "root:letsg0@tcp(10.10.40.2:30083)/audit?parseTime=true&timeout=5s&readTimeout=6s&charset=utf8&parseTime=true&loc=Local", "db addr. ")
)

func InitDb()  {
	var err error

	DB, err = gorm.Open(mysql.Open(*dbaddr), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		klog.Fatalf("init db failed %v. ", err)
	}

	err = DB.AutoMigrate(&AuditInfo{}, &MetaData{})
	if err != nil {
		klog.Fatalf("migrate failed %v. ", err)
	}
}
