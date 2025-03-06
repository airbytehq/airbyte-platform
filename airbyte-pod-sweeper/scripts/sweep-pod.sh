#!/bin/bash
get_job_pods () {
    kubectl -n ${KUBE_NAMESPACE} -L airbyte -l airbyte=job-pod \
      get pods \
      -o=jsonpath='{range .items[*]} {.metadata.name} {.status.phase} {.status.conditions[0].lastTransitionTime} {.status.startTime}{"\n"}{end}'
}
delete_pod() {
    printf "From status '%s' since '%s', " $2 $3
    echo "$1" | grep -v "STATUS" | awk '{print $1}' | xargs --no-run-if-empty kubectl -n ${KUBE_NAMESPACE} delete pod
}
while :
do
    echo "Starting pod sweeper cycle:"

    if [ -n "${RUNNING_TTL_MINUTES}" ]; then
      # Time window for running pods
      RUNNING_DATE_STR=`date -d "now - ${RUNNING_TTL_MINUTES} minutes" --utc -Ins`
      RUNNING_DATE=`date -d $RUNNING_DATE_STR +%s`
      echo "Will sweep running pods from before ${RUNNING_DATE_STR}"
    fi

    if [ -n "${SUCCEEDED_TTL_MINUTES}" ]; then
      # Shorter time window for succeeded pods
      SUCCESS_DATE_STR=`date -d "now - ${SUCCEEDED_TTL_MINUTES} minutes" --utc -Ins`
      SUCCESS_DATE=`date -d $SUCCESS_DATE_STR +%s`
      echo "Will sweep succeeded pods from before ${SUCCESS_DATE_STR}"
    fi

    if [ -n "${UNSUCCESSFUL_TTL_MINUTES}" ]; then
      # Longer time window for unsuccessful pods (to debug)
      NON_SUCCESS_DATE_STR=`date -d "now - ${UNSUCCESSFUL_TTL_MINUTES} minutes" --utc -Ins`
      NON_SUCCESS_DATE=`date -d $NON_SUCCESS_DATE_STR +%s`
      echo "Will sweep unsuccessful pods from before ${NON_SUCCESS_DATE_STR}"
    fi
    (
        IFS=$'\n'
        for POD in `get_job_pods`; do
            IFS=' '
            POD_NAME=`echo $POD | cut -d " " -f 1`
            POD_STATUS=`echo $POD | cut -d " " -f 2`
            POD_DATE_STR=`echo $POD | cut -d " " -f 3`
            POD_START_DATE_STR=`echo $POD | cut -d " " -f 4`
            POD_DATE=`date -d ${POD_DATE_STR:-$POD_START_DATE_STR} '+%s'`
            if [ -n "${RUNNING_TTL_MINUTES}" ] && [ "$POD_STATUS" = "Running" ]; then
              if [ "$POD_DATE" -lt "$RUNNING_DATE" ]; then
                  delete_pod "$POD_NAME" "$POD_STATUS" "$POD_DATE_STR"
              fi
            elif [ -n "${SUCCEEDED_TTL_MINUTES}" ] && [ "$POD_STATUS" = "Succeeded" ]; then
              if [ "$POD_DATE" -lt "$SUCCESS_DATE" ]; then
                  delete_pod "$POD_NAME" "$POD_STATUS" "$POD_DATE_STR"
              fi
            elif [ -n "${UNSUCCESSFUL_TTL_MINUTES}" ] && [ "$POD_STATUS" != "Running" ] && [ "$POD_STATUS" != "Succeeded" ]; then
              if [ "$POD_DATE" -lt "$NON_SUCCESS_DATE" ]; then
                  delete_pod "$POD_NAME" "$POD_STATUS" "$POD_DATE_STR"
              fi
            fi
        done
    )
    echo "Completed pod sweeper cycle.  Sleeping for 60 seconds..."
    sleep 60
done
