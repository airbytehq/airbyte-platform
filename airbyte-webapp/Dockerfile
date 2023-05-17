ARG NGINX_IMAGE=nginx:alpine
FROM ${NGINX_IMAGE}

RUN apk update && apk upgrade && apk upgrade -U openssl
EXPOSE 80

RUN addgroup -S appgroup && \
    adduser -S appuser -G appgroup -u 1000
USER appuser

COPY bin/build /usr/share/nginx/html
COPY bin/nginx/default.conf.template /etc/nginx/templates/default.conf.template
