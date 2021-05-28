module k8s.io/kubernetes/cmd/kubeaudit

go 1.14

require (
	github.com/google/uuid v1.2.0
	github.com/mattbaird/jsonpatch v0.0.0-20200820163806-098863c1fc24
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/exporters/trace/jaeger v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	go.uber.org/zap v1.17.0
	gorm.io/driver/mysql v1.1.0
	gorm.io/driver/sqlite v1.1.4 // indirect
	gorm.io/gorm v1.21.10
	k8s.io/api v0.21.1
	k8s.io/apimachinery v0.21.1
	k8s.io/cli-runtime v0.21.1
	k8s.io/klog v1.0.0
	k8s.io/kubectl v0.21.1
)
