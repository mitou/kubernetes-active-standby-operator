IMAGE ?= ghcr.io/mitou/kubernetes-active-standby-operator
TAG   ?= latest

.PHONY: test vet build docker push

test:
	go test ./...

vet:
	go vet ./...

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/operator .

docker:
	docker build -t $(IMAGE):$(TAG) .

push: docker
	docker push $(IMAGE):$(TAG)
