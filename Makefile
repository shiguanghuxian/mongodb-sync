BINARY=mongodb-sync
VERSION=1.0.0
GOBUILD=go build -ldflags "-X main.VERSION=$(VERSION) -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X main.GIT_HASH=`git rev-parse HEAD`"
DOCKER_NAME=shiguanghuxian/mongodb-sync

default:
	@echo 'Usage of make: [ build | linux | windows | run | docker | docker_push | clean ]'

build: 
	$(GOBUILD) -o ./bin/${BINARY} ./

linux: 
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o ./bin/${BINARY} ./

windows: 
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) -o ./bin/${BINARY}.exe ./

docker: linux
	docker build -t $(DOCKER_NAME) .

docker_push: docker
	docker push $(DOCKER_NAME)
	docker tag $(DOCKER_NAME) $(DOCKER_NAME):$(VERSION)
	docker push $(DOCKER_NAME):$(VERSION)

run: build
	cd bin && ./${BINARY}

clean: 
	rm -f ./bin/${BINARY}
	rm -f ./bin/logs/*

.PHONY: default build linux windows run docker docker_push clean
