ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:alpine3.18
FROM ${NGINX_IMAGE}

EXPOSE 8080

USER root

COPY bin/build /usr/share/nginx/html

RUN <<EOF
find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
chown -R nginx:nginx /usr/share/nginx/html
EOF

COPY bin/nginx/default.conf.template /etc/nginx/templates/default.conf.template

USER nginx:nginx
