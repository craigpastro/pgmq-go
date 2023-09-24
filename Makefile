.PHONY: mockgen
mockgen:
	mockgen --destination mocks/row.go --package=mocks --build_flags=--mod=mod github.com/jackc/pgx/v5 Row
	mockgen --source=pgmq.go --destination mocks/pgmq.go --package=mocks

.PHONY: test
test: mockgen
	go test -race -coverprofile=coverage.out -covermode=atomic
