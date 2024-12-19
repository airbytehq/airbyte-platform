##@ Images:

VERSION ?= $(shell git rev-parse --short HEAD)
IMAGES_DIR := ./oss/docker
OS ?= $(shell uname | tr '[:upper:]' '[:lower:]')

PUBLISH ?= false

BASE_JAVA_IMAGE_VERSION ?= 3.3.1
NGINX_VERSION = 1.27.3
BASE_JAVA_IMAGE = airbyte/airbyte-base-java-image:$(BASE_JAVA_IMAGE_VERSION)

image.base-images: ## Build all the base images
image.base-images: buildx.start
image.base-images: image.airbyte-base-java-image image.airbyte-base-java-python-image image.airbyte-base-java-worker-image

image.airbyte-base-java-image: ## Build the airbyte-base-java-image
image.airbyte-base-java-image: buildx.start
	@if [ "$(PUBLISH)" = "true" ]; then \
		docker buildx build -t airbyte/airbyte-base-java-image:$(VERSION) \
			--builder airbyte-image-builder \
			--platform linux/amd64,linux/arm64 \
			--push \
			$(IMAGES_DIR)/airbyte-base-java-image ; \
	else \
		docker build -t airbyte/airbyte-base-java-image:$(VERSION) $(IMAGES_DIR)/airbyte-base-java-image ; \
	fi

image.airbyte-base-java-python-image: ## Build the airbyte/airbyte-base-java-python-image
image.airbyte-base-java-python-image: buildx.start
	@if [ "$(PUBLISH)" = "true" ]; then \
		docker buildx build -t airbyte/airbyte-base-java-python-image:$(VERSION) \
			--builder airbyte-image-builder \
			--build-arg JDK_IMAGE=$(BASE_JAVA_IMAGE) \
			--platform linux/amd64,linux/arm64 \
			--push \
			$(IMAGES_DIR)/airbyte-base-java-python-image ; \
	else \
		docker build -t airbyte/airbyte-base-java-python-image:$(VERSION) \
			--build-arg JDK_IMAGE=$(BASE_JAVA_IMAGE) \
			$(IMAGES_DIR)/airbyte-base-java-python-image ; \
	fi

image.airbyte-base-java-worker-image: ## Build the airbyte/airbyte-base-java-worker-image
image.airbyte-base-java-worker-image: buildx.start
	@if [ "$(PUBLISH)" = "true" ]; then \
		docker buildx build -t airbyte/airbyte-base-java-worker-image:$(VERSION) \
			--build-arg JDK_IMAGE=$(BASE_JAVA_IMAGE) \
			--platform linux/amd64,linux/arm64 \
			--push \
			$(IMAGES_DIR)/airbyte-base-java-worker-image ; \
	else \
		docker build -t airbyte/airbyte-base-java-worker-image:$(VERSION) \
			--build-arg JDK_IMAGE=$(BASE_JAVA_IMAGE) \
			$(IMAGES_DIR)/airbyte-base-java-worker-image ; \
	fi

image.airbyte-nginx-slim: ## Build the airbyte/nginx-unprivileged image
image.airbyte-nginx-slim: buildx.start
	@if [ "$(PUBLISH)" = "true" ]; then \
		docker buildx build -t airbyte/nginx-alpine-slim:$(NGINX_VERSION) \
			--build-arg UID="1000" \
			--build-arg GID="1000" \
			--platform linux/amd64,linux/arm64 \
			--push \
			https://github.com/nginxinc/docker-nginx-unprivileged.git\#$(NGINX_VERSION):mainline/alpine-slim ; \
	else \
		docker build -t airbyte/nginx-alpine-slim:$(NGINX_VERSION) \
			--build-arg UID="1000" \
			--build-arg GID="1000" \
			https://github.com/nginxinc/docker-nginx-unprivileged.git\#$(NGINX_VERSION):mainline/alpine-slim ; \
	fi
