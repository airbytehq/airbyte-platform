ARG BUILD_IMAGE
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:alpine3.18

FROM ${BUILD_IMAGE} AS builder

FROM ${NGINX_IMAGE}

EXPOSE 80

ARG SRC_DIR=/workspace/oss/airbyte-webapp/build/app/build/app

COPY --from=builder ${SRC_DIR} /usr/share/nginx/html
RUN find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template
