ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:alpine3.20
FROM ${NGINX_IMAGE}

ARG BUILD_DIR=bin/build
ARG NGINX_CONFIG=bin/nginx/default.conf.template

EXPOSE 8080

USER root

COPY ${BUILD_DIR} /usr/share/nginx/html

RUN <<EOF
find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
chown -R nginx:nginx /usr/share/nginx/html
EOF

COPY ${NGINX_CONFIG} /etc/nginx/templates/default.conf.template

USER nginx:nginx
