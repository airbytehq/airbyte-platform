GIT_REF ?= master
VERSION ?= $(shell git rev-parse --short HEAD)
APP_VERSION ?= $(VERSION)

.PHONY: gen.config
gen.config:
	via helm gen-config-templates --config config.yaml --out-dir ./templates/config --chart-name airbyte

.PHONY: helm.publish
helm.publish:
	via helm publish-oss-v2 \
		--chart-version $(VERSION) \
		--app-version $(APP_VERSION) \
		--ref $(GIT_REF)
