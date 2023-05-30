. tools/lib/lib.sh
. tools/bin/workflow/local/_k8s.sh

CLUSTER_IP=$(colima -p ab-control-plane list -j | jq --raw-output .address)

KUBE_CONTEXT=${KUBE_CONTEXT:=$(kubectl config current-context)}
KUBE_CONTEXT_DATA_PLANE=${KUBE_CONTEXT_DATA_PLANE:=$KUBE_CONTEXT}

IMAGE_TAG=${IMAGE_TAG:=dev}
if [[ $IMAGE_TAG == "dev" ]]; then 
    ensure_image_exists airbyte/bootloader:dev
    ensure_image_exists airbyte/connector-builder-server:dev
    ensure_image_exists airbyte/server:dev
    ensure_image_exists airbyte/webapp:dev
    ensure_image_exists airbyte/workers:dev
fi
