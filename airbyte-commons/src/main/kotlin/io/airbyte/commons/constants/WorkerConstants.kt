/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants

import java.time.Duration

/**
 * Worker constants.
 */
object WorkerConstants {
  const val RESET_JOB_SOURCE_DOCKER_IMAGE_STUB: String = "airbyte_empty"

  const val ATTEMPT_ID: String = "ATTEMPT_ID"
  const val JOB_ID: String = "JOB_ID"

  const val DD_ENV_VAR: String = (
    "-XX:+ExitOnOutOfMemoryError " +
      "-XX:MaxRAMPercentage=75.0 " +
      "-javaagent:/airbyte/dd-java-agent.jar " +
      "-Ddd.profiling.enabled=true " +
      "-XX:FlightRecorderOptions=stackdepth=256 " +
      "-Ddd.trace.sample.rate=0.5 " +
      "-Ddd.trace.request_header.tags=User-Agent:http.useragent"
  )

  object KubeConstants {
    val INIT_CONTAINER_STARTUP_TIMEOUT: Duration = Duration.ofMinutes(15)
    val INIT_CONTAINER_TERMINATION_TIMEOUT: Duration = Duration.ofMinutes(2)
    val POD_READY_TIMEOUT: Duration = Duration.ofMinutes(2)

    /**
     * If changing the value for this, make sure to update the deadline values in
     * [io.airbyte.workload.handler.WorkloadHandlerImpl] as well.
     */
    val FULL_POD_TIMEOUT: Duration = INIT_CONTAINER_STARTUP_TIMEOUT.plus(INIT_CONTAINER_TERMINATION_TIMEOUT).plus(POD_READY_TIMEOUT)
  }
}
