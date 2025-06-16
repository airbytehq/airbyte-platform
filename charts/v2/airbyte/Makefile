##@ Helm V2 (OSS):

OSS_MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
GIT_REF ?= master
VERSION ?= $(shell git rev-parse --short HEAD)
APP_VERSION ?= $(VERSION)

.PHONY: gen.config.oss
gen.config.oss: ## Generate V2 OSS config templates
	via helm gen-config-templates \
		--config $(OSS_MAKEFILE_DIR)/config.yaml \
		--out-dir $(OSS_MAKEFILE_DIR)/templates/config \
		--chart-name airbyte

.PHONY: helm.publish.oss
helm.publish.oss: ## Publish the V2 OSS chart
	via helm publish-oss-v2 \
		--chart-version $(VERSION) \
		--app-version $(APP_VERSION) \
		--ref $(GIT_REF)
