FROM registry.woqutech.com/library/golang:1.14 as builder
WORKDIR /GoProject/src/k8s.io/kubernetes/cmd/kubeaudit
ENV GOPATH=/GoProject
ENV GO111MODULE=off

COPY .  /GoProject/src/k8s.io/kubernetes/cmd/kubeaudit/
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o  _output/kubeaudit main.go

FROM registry.woqutech.com/google_containers/alpine:3.10 as prod
LABEL maintainers="longhui.li"
LABEL description="kube audit"
ARG version
COPY --from=0 /GoProject/src/k8s.io/kubernetes/cmd/kubeaudit/_output/kubeaudit /kubeaudit

ENTRYPOINT ["/kubeaudit"]
ENV COMMITID=$version
