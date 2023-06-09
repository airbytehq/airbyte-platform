ARG NGINX_IMAGE=nginx:alpine
FROM ${NGINX_IMAGE}

RUN apk update && apk upgrade && apk upgrade -U openssl
EXPOSE 80

RUN touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid && \
    chown -R nginx:nginx /var/cache/nginx


#USER nginx

COPY bin/build /usr/share/nginx/html
COPY bin/nginx/default.conf.template /etc/nginx/templates/default.conf.template
