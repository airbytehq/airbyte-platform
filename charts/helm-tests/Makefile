.PHONY:
test.unit:
	go test ./tests -tags=template -v

.PHONY:
test.integration:
	go test ./tests -tags=install -v

.PHONY:
test.clean:
	go clean -testcache
