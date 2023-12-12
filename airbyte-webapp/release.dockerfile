ARG BUILD_IMAGE
FROM ${BUILD_IMAGE} AS builder


FROM nginx:alpine AS release 

EXPOSE 80

ARG SRC_DIR=/workspace/oss/airbyte-webapp/build/app/build/app

COPY --from=builder ${SRC_DIR} /usr/share/nginx/html
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
