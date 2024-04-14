ARG BUILD_IMAGE
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:alpine3.18

FROM ${BUILD_IMAGE} AS builder

FROM ${NGINX_IMAGE}

EXPOSE 8080

ARG SRC_DIR=/workspace/oss/airbyte-webapp/build/app/build/app

USER root
COPY --from=builder ${SRC_DIR} /usr/share/nginx/html
RUN find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
RUN chown -R nginx:nginx /usr/share/nginx/html
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template

USER nginx:nginx
