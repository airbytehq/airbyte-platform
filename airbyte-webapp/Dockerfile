ARG NGINX_IMAGE=nginx:alpine
FROM ${NGINX_IMAGE}

EXPOSE 80

COPY bin/build /usr/share/nginx/html
RUN find /usr/share/nginx/html -type d -exec chmod 755 '{}' \; -o -type f -exec chmod 644 '{}' \;
COPY bin/nginx/default.conf.template /etc/nginx/templates/default.conf.template
