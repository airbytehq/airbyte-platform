ARG BUILD_IMAGE
FROM ${BUILD_IMAGE} AS builder


FROM nginx:alpine AS release 

EXPOSE 80

ARG SRC_DIR=/workspace/oss/airbyte-webapp/build/app/build/app

COPY --from=builder ${SRC_DIR} /usr/share/nginx/html
RUN find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
