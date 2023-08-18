#!/bin/bash

rm /etc/nginx/nginx.conf

# Check if PROXY_TEST is set and its value to decide on the template
if [[ -n "${PROXY_TEST}" ]]; then
    case "${PROXY_TEST}" in
        "AUTH")
            echo "Running in test mode with auth"
            htpasswd -c -b /etc/nginx/.htpasswd $BASIC_AUTH_USERNAME $BASIC_AUTH_PASSWORD
            TEMPLATE_PATH="/etc/nginx/templates/nginx-test-auth.conf.template"
            ;;
        "NEW_AUTH")
            echo "Running in test mode with new auth"
            htpasswd -c -b /etc/nginx/.htpasswd $BASIC_AUTH_USERNAME $BASIC_AUTH_PASSWORD
            TEMPLATE_PATH="/etc/nginx/templates/nginx-test-auth-newpass.conf.template"
            ;;
        "NO_AUTH")
            echo "Running in test mode without auth"
            TEMPLATE_PATH="/etc/nginx/templates/nginx-test-no-auth.conf.template"
            ;;
        *)
            echo "Invalid PROXY_TEST value. Expected AUTH, NEW_AUTH, or NO_AUTH."
            exit 1
            ;;
    esac
else
    if [[ -z "${BASIC_AUTH_USERNAME}" ]]; then
        echo "BASIC_AUTH_USERNAME is not set, using production config without auth"
        TEMPLATE_PATH="/etc/nginx/templates/nginx-no-auth.conf.template"
    else
        echo "BASIC_AUTH_USERNAME is set, using production config with auth for user '$BASIC_AUTH_USERNAME'"
        htpasswd -c -b /etc/nginx/.htpasswd $BASIC_AUTH_USERNAME $BASIC_AUTH_PASSWORD
        TEMPLATE_PATH="/etc/nginx/templates/nginx-auth.conf.template"
    fi
fi

envsubst '${PROXY_PASS_WEB} ${PROXY_PASS_API} ${CONNECTOR_BUILDER_SERVER_API} ${PROXY_PASS_AIRBYTE_API_SERVER} ${PROXY_PASS_RESOLVER} ${BASIC_AUTH_PROXY_TIMEOUT}' < $TEMPLATE_PATH > /etc/nginx/nginx.conf
echo "starting nginx..."
nginx -v
nginx -g "daemon off;"
