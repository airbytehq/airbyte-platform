#!/usr/bin/env bash

<<comment
This script deploys Airbyte Cloud to your local kubernetes cluster, sets up proper port-forwarding, and runs a local firebase emulator.
This script does not exit until it is manually killed, as firebase emulator output is needed to perform proper authentication
in the cloud UI, and because if the firebase emulator is not torn down between startups of the app, email conflicts can occur.

Pressing Ctrl-C will shut down the local airbyte cloud deploy and all associated resources that it started up.
comment

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CLEAR='\033[0m'

function ensureFirebaseCliInstalled {
  if ! command -v firebase &> /dev/null
  then
      echo -e "${RED}Firebase CLI is not installed. This is required to run Airbyte Cloud locally."
      echo -e "Please install this by running the following command, and try again:"
      echo -e "${BLUE}curl -sL https://firebase.tools | bash${CLEAR}"
      exit 1
  fi
}

function ensureDockerDesktop {
  echo "Switching to docker-desktop kube context..." >&2
  # Switch to docker-desktop to ensure that any changes are only applied locally
  kubectl config use-context docker-desktop
  dockerDesktopContextExitCode=$?
  if [ $dockerDesktopContextExitCode -ne 0 ]; then
    echo -e "${RED}Could not switch to docker-desktop kube context. Ensure that Docker Desktop is running and Kubernetes is enabled: https://docs.docker.com/desktop/kubernetes/${CLEAR}" >&2
    exit $dockerDesktopContextExitCode
  fi
}

ensureFirebaseCliInstalled
ensureDockerDesktop

function cleanup {
  ensureDockerDesktop

  echo "Tearing down local helm deployment..." >&2
  helm uninstall ab --wait

  echo "Killing port-forward of webapp proxy..." >&2
  kill $webappProxyPid 2> /dev/null

  echo "Killing port-forward of cloud API proxy..." >&2
  kill $cloudApiProxyPid 2> /dev/null

  echo "Killing port-forward of config db proxy..." >&2
  kill $configDbProxyPid 2> /dev/null

  echo "Killing port-forward of cloud db proxy..." >&2
  kill $cloudDbProxyPid 2> /dev/null

  echo "Killing firebase emulator..." >&2
  kill $firebaseEmulatorPid 2> /dev/null
  sleep 2
}
trap cleanup EXIT


function ensureAirbyteCloudWorkingDirectory {
  # set working directory to cloud repo root, which is two levels above this script
  cd "$(dirname "$0")/../.."

  # fail fast if the working directory isn't airbyte-platform-internal (maybe the script moved relative to the repo root?)
  if test ${PWD##*/} != 'airbyte-platform-internal'; then
    echo -e "${RED}Working directory isn't airbyte-platform-internal, perhaps this script moved?${CLEAR}"
    exit 1
  fi
}

ensureAirbyteCloudWorkingDirectory


# This pod is sometimes not deleted if local deployment is shut down incorrectly - swallow error that is printed if this pod does not exist
kubectl delete pod airbyte-minio-create-bucket --force --context=docker-desktop 2> /dev/null

echo "Creating jobs namespace if necessary..."
kubectl create namespace jobs --context=docker-desktop 2> /dev/null

set -e

echo "Cleaning up any existing jobs pods to avoid job id conflicts..."
kubectl delete --all pods --namespace=jobs --force --context=docker-desktop

# Kill the kubectl portforwards and firebase emulator if still running for any reason
echo "Killing any existing processes running on desired ports..."
lsof -t -i :9099 -i :4000 -i :8002 -i :8004 -i :8008 -i :8009 -i :8080 | xargs kill -9

echo "Deploying airbyte cloud to local kube cluster..."
pushd infra/kube/airbyte && helm dep update && popd && kubectl create namespace ab && kubectl apply -f infra/kube/service-account.yaml && kubectl patch -n ab serviceaccount airbyte-admin -p '{"metadata": {"annotations": {"helm.sh/resource-policy": "keep"}}}'
helm install --debug ab ./infra/kube/airbyte
helmInstallExitCode=$?
if [ $helmInstallExitCode -ne 0 ]; then
  echo "Helm deploy failed with exit code $helmInstallExitCode" >&2
  exit $dockerDesktopContextExitCode
fi

while [[ $(kubectl get pods --context=docker-desktop | grep -v Running | grep -v Completed | tail -n +2) ]]; do echo "Waiting for all pods to be running..."; sleep 5; done

echo "Port forwarding webapp proxy to port 8004..."
kubectl port-forward svc/airbyte-proxy-svc 8004:80 --context=docker-desktop &
webappProxyPid=$!
sleep 1

echo "Port forwarding cloud API proxy to port 8002..."
kubectl port-forward svc/airbyte-cloud-proxy-svc 8002:80 --context=docker-desktop &
cloudApiProxyPid=$!
sleep 1

echo "Port forwarding cloud public-api proxy to port 8080..."
kubectl port-forward svc/airbyte-cloud-public-api-server-svc 8080:80 --context=docker-desktop &
cloudApiProxyPid=$!
sleep 1

echo "Port forwarding config db to port 8008..."
kubectl port-forward svc/airbyte-db-svc 8008:5432 --context=docker-desktop &
configDbProxyPid=$!
sleep 1

echo "Port forwarding cloud db to port 8009..."
kubectl port-forward svc/cloud-db-svc 8009:5432 --context=docker-desktop &
cloudDbProxyPid=$!
sleep 1


while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8004/api/v1/health)" != "200" ]]; do echo "Waiting for webapp to become available..."; sleep 5; done
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8002/cloud/v1/health)" != "200" ]]; do echo "Waiting for cloud server to become available..."; sleep 5; done
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8080/health)" != "200" ]]; do echo "Waiting for cloud public-api server to become available..."; sleep 5; done

echo "Starting firebase emulator..."
firebase emulators:start --project demo-test --only auth --import ./firebase-local --export-on-exit &
firebaseEmulatorPid=$!

while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:9099)" != "200" ]]; do echo "Waiting for firebase emulator to become available..."; sleep 5; done

sleep 2

echo -e "${GREEN}------ Airbyte Cloud has been deployed locally! It can be accessed at http://localhost:8004 ------"
echo -e "Keep this terminal window open to continue running local cloud."
echo -e "Navigate to the signup page at http://localhost:8004/signup to create a user."
echo -e "Email verification requests will appear in this terminal:\n  just Cmd-Click the link to verify and then refresh the localhost:8004 page."
echo -e "To shut down the local cloud deployment, press Ctrl-C. This will shut down all local cloud resources.${CLEAR}"

wait $firebaseEmulatorPid
