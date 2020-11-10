BINARY=mongodb-sync

default:
	@echo 'Usage of make: [ build | linux | windows | run | docker | docker_push | clean ]'

build: 
	go build -o ./bin/${BINARY} ./

linux: 
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/${BINARY} ./

windows: 
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./bin/${BINARY}.exe ./

docker: linux
	docker build -t shiguanghuxian/mongodb-sync .

docker_push: docker
	docker push shiguanghuxian/mongodb-sync

run: build
	cd bin && ./${BINARY}

clean: 
	rm -f ./bin/${BINARY}
	rm -f ./bin/logs/*

.PHONY: default build linux windows run docker docker_push clean
