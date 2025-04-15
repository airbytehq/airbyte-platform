GIT_REF ?= master
VERSION ?= $(shell git rev-parse --short HEAD)
APP_VERSION ?= $(VERSION)

.PHONY: gen.config
gen.config:
	via helm gen-config-templates --config config.yaml --out-dir ./templates/config --chart-name airbyte-data-plane --base-secret-path secretName

.PHONY: helm.publish
helm.publish:
	via helm publish \
		--version $(VERSION) \
		--app-version $(APP_VERSION) \
		--name airbyte/airbyte-data-plane \
		--source-path oss/charts/v2/airbyte-data-plane \
		--src-repo https://github.com/airbytehq/airbyte-platform-internal.git \
		--dst-repo https://github.com/airbytehq/helm-charts.git \
		--ref $(GIT_REF)
