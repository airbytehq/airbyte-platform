/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.workers.pod.FileConstants.CATALOG_FILE
import io.airbyte.workers.pod.FileConstants.CONFIG_DIR
import io.airbyte.workers.pod.FileConstants.CONNECTOR_CONFIG_FILE
import io.airbyte.workers.pod.FileConstants.DEST_DIR
import io.airbyte.workers.pod.FileConstants.EXIT_CODE_FILE
import io.airbyte.workers.pod.FileConstants.INPUT_STATE_FILE
import io.airbyte.workers.pod.FileConstants.JOB_OUTPUT_FILE
import io.airbyte.workers.pod.FileConstants.SOURCE_DIR
import io.airbyte.workers.pod.FileConstants.STDERR_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.STDIN_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.STDOUT_PIPE_FILE
import io.airbyte.workers.pod.FileConstants.TERMINATION_MARKER_FILE

/**
 * Factory for generating/templating the main shell scripts we use as the entry points in our containers.
 * Factor out into Singleton as necessary.
 */
internal object ContainerCommandFactory {
  // WARNING: Fragile. Coupled to our conventions on building, unpacking and naming our executables.
  private const val SIDE_CAR_APPLICATION_EXECUTABLE = "/app/airbyte-app/bin/airbyte-connector-sidecar"
  private const val ORCHESTRATOR_APPLICATION_EXECUTABLE = "/app/airbyte-app/bin/airbyte-container-orchestrator"
  private const val PROFILER_APPLICATION_EXECUTABLE = "/app/airbyte-app/bin/airbyte-async-profiler"
  private const val TERMINATION_CHECK_INTERVAL_SECONDS = 10

  /**
   * Runs the sidecar container and creates the TERMINATION_MARKER_FILE file on exit for both success and failure.
   */
  fun sidecar() =
    """
    trap "touch $TERMINATION_MARKER_FILE" EXIT
    $SIDE_CAR_APPLICATION_EXECUTABLE
    """.trimIndent()

  fun profiler() =
    """
    trap "touch $TERMINATION_MARKER_FILE" EXIT
    $PROFILER_APPLICATION_EXECUTABLE
    """.trimIndent()

  /**
   * Runs the orchestrator and creates the TERMINATION_MARKER_FILE file on exit for both success and failure.
   */
  fun orchestrator() =
    """
    trap "touch $SOURCE_DIR/$TERMINATION_MARKER_FILE $DEST_DIR/$TERMINATION_MARKER_FILE" EXIT
    $ORCHESTRATOR_APPLICATION_EXECUTABLE
    """.trimIndent()

  /**
   * Starts the connector operation and a monitor process that conditionally kills the connector and exits if
   * a TERMINATION_MARKER_FILE is present. This ensures the connector exits when the sidecar exits.
   */
  fun connectorOperation(
    operationCommand: String,
    configArgs: String,
  ) = connectorCommandWrapper(
    """
    eval "${'$'}AIRBYTE_ENTRYPOINT $operationCommand $configArgs" > $CONFIG_DIR/$JOB_OUTPUT_FILE
    """.trimIndent(),
  )

  @SuppressWarnings("LineLength")
  fun source() =
    connectorCommandWrapper(
      """
      # only provide the state flag if present
      if [ ! -f $SOURCE_DIR/$INPUT_STATE_FILE ]; then
        eval "${'$'}AIRBYTE_ENTRYPOINT read --config $SOURCE_DIR/${CONNECTOR_CONFIG_FILE} --catalog $SOURCE_DIR/${CATALOG_FILE}" 2> $STDERR_PIPE_FILE > $STDOUT_PIPE_FILE
      else
        eval "${'$'}AIRBYTE_ENTRYPOINT read --config $SOURCE_DIR/${CONNECTOR_CONFIG_FILE} --catalog $SOURCE_DIR/${CATALOG_FILE} --state $SOURCE_DIR/$INPUT_STATE_FILE" 2> $STDERR_PIPE_FILE > $STDOUT_PIPE_FILE
      fi
      """.trimIndent(),
    )

  @SuppressWarnings("LineLength")
  fun destination() =
    connectorCommandWrapper(
      """
      eval "${'$'}AIRBYTE_ENTRYPOINT write --config $DEST_DIR/${CONNECTOR_CONFIG_FILE} --catalog $DEST_DIR/${CATALOG_FILE}" 2> $STDERR_PIPE_FILE > $STDOUT_PIPE_FILE < $STDIN_PIPE_FILE
      """.trimIndent(),
    )

  private fun connectorCommandWrapper(command: String): String =
    """
    # fail loudly if entry point not set
    if [ -z "${'$'}AIRBYTE_ENTRYPOINT" ]; then
      echo "Entrypoint was not set! AIRBYTE_ENTRYPOINT must be set in the container."
      exit 127
    else
      echo "Using AIRBYTE_ENTRYPOINT: ${'$'}AIRBYTE_ENTRYPOINT"
    fi

    # run connector in background and store PID
    ($command) &
    CHILD_PID=${'$'}!

    # run busy loop in background that checks for termination file and if present kills the connector operation and exits
    (while true; do if [ -f $TERMINATION_MARKER_FILE ]; then kill ${'$'}CHILD_PID; exit 0; fi; sleep $TERMINATION_CHECK_INTERVAL_SECONDS; done) &

    # wait on connector operation
    wait ${'$'}CHILD_PID
    EXIT_CODE=$?

    # write its exit code to a file for the sidecar
    echo ${'$'}EXIT_CODE > TEMP_EXIT_CODE.txt
    # use a swap file to make creation and writing atomic
    mv TEMP_EXIT_CODE.txt $EXIT_CODE_FILE

    # propagate connector exit code by assuming it
    exit ${'$'}EXIT_CODE
    """.trimIndent()
}
