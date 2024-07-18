package io.airbyte.workload.launcher.pods.factories

import io.airbyte.workers.process.KubePodProcess
import io.airbyte.workers.sync.OrchestratorConstants

/**
 * Factory for generating/templating the main shell scripts we use as the entry points in our containers.
 * Factor out into Singleton as necessary.
 */
object ContainerCommandFactory {
  private const val TERMINATION_MARKER_FILE = "TERMINATED"

  // WARNING: Fragile. Coupled to our conventions on building, unpacking and naming our executables.
  private const val SIDE_CAR_APPLICATION_EXECUTABLE = "/app/airbyte-app/bin/airbyte-connector-sidecar"
  private const val TERMINATION_CHECK_INTERVAL_SECONDS = 10

  /**
   * Runs the sidecar container and creates the TERMINATION_MARKER_FILE file on exit for both success and failure.
   */
  fun sidecar() =
    """
    trap "touch $TERMINATION_MARKER_FILE" EXIT
    $SIDE_CAR_APPLICATION_EXECUTABLE
    """.trimIndent()

  /**
   * Starts the connector operation and a monitor process that conditionally kills the connector and exits if
   * a TERMINATION_MARKER_FILE is present. This ensures the connector exits when the sidecar exits.
   */
  fun connectorOperation(
    operationCommand: String,
    configArgs: String,
  ) = """
    # fail loudly if entry point not set
    if [ -z "${'$'}AIRBYTE_ENTRYPOINT" ]; then
      echo "Entrypoint was not set! AIRBYTE_ENTRYPOINT must be set in the container to run on Kubernetes."
      exit 127
    else
      echo "Using existing AIRBYTE_ENTRYPOINT: ${'$'}AIRBYTE_ENTRYPOINT"
    fi

    # run connector operation in background and store PID
    (eval "${'$'}AIRBYTE_ENTRYPOINT $operationCommand $configArgs" > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}) &
    CHILD_PID=${'$'}!

    # run busy loop in background that checks for termination file and if present kills the connector operation and exits
    (while true; do if [ -f $TERMINATION_MARKER_FILE ]; then kill ${'$'}CHILD_PID; exit 0; fi; sleep $TERMINATION_CHECK_INTERVAL_SECONDS; done) &

    # wait on connector operation
    wait ${'$'}CHILD_PID
    EXIT_CODE=$?

    # write its exit code to a file for the sidecar
    echo ${'$'}EXIT_CODE > ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.EXIT_CODE_FILE}

    # print result for debugging
    cat ${KubePodProcess.CONFIG_DIR}/${OrchestratorConstants.JOB_OUTPUT_FILENAME}

    # propagate connector exit code by assuming it
    exit ${'$'}EXIT_CODE
    """.trimIndent()
}
