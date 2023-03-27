lint:
	golangci-lint run
	
test:
	go clean --testcache && go test ./...

race:
	go clean --testcache && go test -race ./...

dbcounts:
	go run main.go -vv -countsonly -dbport 3307

auto:
	go run main.go -v -auto -dryrun -dbport 3307

livetest:
	go clean -testcache && LIVETEST=true go test ./...

testimport:
	go run main.go -vvv -datafolder "./testdata" -cleardown -dbport 3307

import:
	go run main.go -v -download -auto -dbport 3307

sequential:
	go run main.go -v -download -dbport 3307

concurrent:
	go run main.go -v -download -concurrent -dbport 3307

docker:
	docker-compose down --volumes
	docker-compose up --build \
	--remove-orphans \
    --exit-code-from importer-app
	docker-compose down --volumes
