ARG NGINX_IMAGE=airbyte/airbyte-base-nginx-image:1.27.3-2
FROM ${NGINX_IMAGE}

ARG BUILD_DIR=bin/build
ARG NGINX_CONFIG=bin/nginx/default.conf.template

EXPOSE 8080

COPY --chown=nginx:nginx ${BUILD_DIR} /usr/share/nginx/html
COPY --chown=nginx:nginx ${NGINX_CONFIG} /etc/nginx/templates/default.conf.template

USER root

RUN <<EOF
find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
EOF

USER nginx:nginx
