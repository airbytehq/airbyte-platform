.PHONY:
v2.test.unit:
	go test ./tests/v2/... -v -count=1 -timeout 0s

.PHONY:
v2-data-plane.test.unit:
	go test ./tests/v2-data-plane/... -v -count=1 -timeout 0s

.PHONY:
v2.test.clean:
	go clean -testcache
