ARG NGINX_IMAGE=nginx:alpine
FROM ${NGINX_IMAGE}

COPY bin/build /usr/share/nginx/html
COPY bin/nginx/default.conf.template /etc/nginx/templates/default.conf.template


