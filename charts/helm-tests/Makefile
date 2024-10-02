.PHONY:
test.unit:
	go test ./tests/... -v -count=1 -timeout 0s

.PHONY:
test.integration:
	go test ./integration_tests/... -v -count=1 -timeout 0s

.PHONY:
test.clean:
	go clean -testcache
