.PHONY:
v1.test.unit:
	go test ./tests/v1/... -v -count=1 -timeout 0s

.PHONY:
v1.test.integration:
	go test ./integration_tests/v1/... -v -count=1 -timeout 0s

.PHONY:
v1.test.clean:
	go clean -testcache

.PHONY:
v2.test.unit:
	go test ./tests/v2/... -v -count=1 -timeout 0s

.PHONY:
v2.test.clean:
	go clean -testcache
