BINARY=mongodb-sync
VERSION=1.0.0

default:
	@echo 'Usage of make: [ build | linux | windows | run | docker | docker_push | clean ]'

build: 
	go build -ldflags "-X main.VERSION=$(VERSION) -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X main.GIT_HASH=`git rev-parse HEAD`" -o ./bin/${BINARY} ./

linux: 
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X main.VERSION=$(VERSION) -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X main.GIT_HASH=`git rev-parse HEAD`" -o ./bin/${BINARY} ./

windows: 
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags "-X main.VERSION=$(VERSION) -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X main.GIT_HASH=`git rev-parse HEAD`" -o ./bin/${BINARY}.exe ./

docker: linux
	docker build -t shiguanghuxian/mongodb-sync .

docker_push: docker
	docker push shiguanghuxian/mongodb-sync
	docker tag shiguanghuxian/mongodb-sync shiguanghuxian/mongodb-sync:$(VERSION)
	docker push shiguanghuxian/mongodb-sync:$(VERSION)

run: build
	cd bin && ./${BINARY}

clean: 
	rm -f ./bin/${BINARY}
	rm -f ./bin/logs/*

.PHONY: default build linux windows run docker docker_push clean
