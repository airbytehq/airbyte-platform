#!/bin/bash

NAME="airbyte-proxy-test-container"
BASIC_AUTH_USERNAME=airbyte
BASIC_AUTH_PASSWORD=password
BASIC_AUTH_UPDATED_PASSWORD=pa55w0rd
BASIC_AUTH_PROXY_TIMEOUT=120

if [[ -z "${DAGGER}" ]]; then
    # If DAGGER env variable is not set
    TEST_HOST_AUTH="localhost"
    TEST_HOST_NEWAUTH="localhost"
    TEST_HOST_NOAUTH="localhost"
else
    # If DAGGER env variable is set, set TEST_HOST based on the test section
    # These correspond to the hostnames we spin up in the dagger config
    TEST_HOST_AUTH="airbyte-proxy-test-container"
    TEST_HOST_NEWAUTH="airbyte-proxy-test-container-newauth"
    TEST_HOST_NOAUTH="airbyte-proxy-test-container-noauth"
fi

VERSION="${VERSION:-dev}" # defaults to "dev", otherwise it is set by environment's $VERSION

echo "testing with proxy container airbyte/proxy:$VERSION"

function start_container () {
  CMD="docker run -d -p $1:8000 --env BASIC_AUTH_USERNAME=$2 --env BASIC_AUTH_PASSWORD=$3 --env BASIC_AUTH_PROXY_TIMEOUT=$4 --env PROXY_PASS_WEB=http://localhost --env PROXY_PASS_API=http://localhost --env CONNECTOR_BUILDER_SERVER_API=http://localhost --env PROXY_PASS_AIRBYTE_API_SERVER=http://localhost --name $NAME-$1 airbyte/proxy:$VERSION"
  echo $CMD
  eval $CMD
  wait_for_docker $NAME-$1;
  sleep 2
}

function stop_container () {
  echo "Stopping $1"
  docker kill $1
  docker rm $1
}

function wait_for_docker() {
  until [ "`docker inspect -f {{.State.Running}} $1`"=="true" ]; do
    sleep 1;
  done;
  sleep 1;
}

# If using Dagger in CI, we manage the lifecycle of the containers through that
# and assume they are already up. We also don't care if the service itself is up,
# only that the proxy is working.
# Here we preserve the old way for backwards compatibility
echo "Testing airbyte proxy..."
# Start container with port 8000
if [[ -z "$DAGGER" ]]; then
    start_container 8000 $BASIC_AUTH_USERNAME $BASIC_AUTH_PASSWORD $BASIC_AUTH_PROXY_TIMEOUT
fi

# Test on port 8000
echo "Testing access without auth"
RESPONSE=`curl "http://$TEST_HOST_AUTH:8000" -i -v`
if [[ $RESPONSE == *"401 Unauthorized"* ]]; then
  echo "✔️  access without auth blocked"
else
  echo "Auth not working"
  echo $RESPONSE
  exit 1
fi

echo "Testing access with auth"
RESPONSE=`curl "http://$BASIC_AUTH_USERNAME:$BASIC_AUTH_PASSWORD@$TEST_HOST_AUTH:8000" -i -v`
if [[ $RESPONSE != *"401 Unauthorized"* ]]; then
  echo "✔️  access with auth worked"
else
  echo "Auth not working"
  echo $RESPONSE
  exit 1
fi

# Start container with updated password on port 8001
if [[ -z "$DAGGER" ]]; then
    stop_container $NAME-8000
    start_container 8001 $BASIC_AUTH_USERNAME $BASIC_AUTH_UPDATED_PASSWORD $BASIC_AUTH_PROXY_TIMEOUT
fi

# Test on port 8001
echo "Testing access with original password"
RESPONSE=`curl "http://$BASIC_AUTH_USERNAME:$BASIC_AUTH_PASSWORD@$TEST_HOST_NEWAUTH:8001" -i -v`
if [[ $RESPONSE == *"401 Unauthorized"* ]]; then
  echo "✔️  access with original auth blocked"
else
  echo "Auth not working"
  echo $RESPONSE
  exit 1
fi

echo "Testing access updated auth"
RESPONSE=`curl "http://$BASIC_AUTH_USERNAME:$BASIC_AUTH_UPDATED_PASSWORD@$TEST_HOST_NEWAUTH:8001" -i -v`
if [[ $RESPONSE != *"401 Unauthorized"* ]]; then
  echo "✔️  access with updated auth worked"
else
  echo "Auth not working"
  echo $RESPONSE
  exit 1
fi

# Start container with no password on port 8002
if [[ -z "$DAGGER" ]]; then
    stop_container $NAME-8001
    start_container 8002 "" ""
fi

# Test on port 8002
echo "Testing access without auth"
RESPONSE=`curl "http://$TEST_HOST_NOAUTH:8002" -i -v`
if [[ $RESPONSE != *"401 Unauthorized"* ]]; then
  echo "✔️  access without auth allowed when configured"
else
  echo "Auth not working"
  echo $RESPONSE
  exit 1
fi

if [[ -z "$DAGGER" ]]; then
    stop_container $NAME-8002
fi

# TODO: We can't test external URLs without a resolver, but adding a resolver that isn't dynamic+local doesn't work with docker.

# echo "Testing that PROXY_PASS can be used to change the backend"
# start_container_with_proxy "http://www.google.com"

# RESPONSE=`curl "http://$TEST_HOST:$PORT" -i --silent`
# if [[ $RESPONSE == *"google.com"* ]]; then
#   echo "✔️  proxy backends can be changed"
# else
#   echo "Proxy update not working"
#   echo $RESPONSE
#   exit 1
# fi

echo "Tests Passed ✅"
exit 0
