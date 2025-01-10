/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants;

import java.time.Duration;

/**
 * Worker constants.
 */
public class WorkerConstants {

  public static final String RESET_JOB_SOURCE_DOCKER_IMAGE_STUB = "airbyte_empty";

  public static final String ATTEMPT_ID = "ATTEMPT_ID";
  public static final String JOB_ID = "JOB_ID";

  public static final String DD_ENV_VAR = "-XX:+ExitOnOutOfMemoryError "
      + "-XX:MaxRAMPercentage=75.0 "
      + "-javaagent:/airbyte/dd-java-agent.jar "
      + "-Ddd.profiling.enabled=true "
      + "-XX:FlightRecorderOptions=stackdepth=256 "
      + "-Ddd.trace.sample.rate=0.5 "
      + "-Ddd.trace.request_header.tags=User-Agent:http.useragent";

  public static class KubeConstants {

    public static final Duration INIT_CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(15);
    public static final Duration INIT_CONTAINER_TERMINATION_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration POD_READY_TIMEOUT = Duration.ofMinutes(2);
    /**
     * If changing the value for this, make sure to update the deadline values in
     * {@link io.airbyte.workload.handler.WorkloadHandlerImpl} as well.
     */
    public static final Duration FULL_POD_TIMEOUT = INIT_CONTAINER_STARTUP_TIMEOUT.plus(INIT_CONTAINER_TERMINATION_TIMEOUT).plus(POD_READY_TIMEOUT);

  }

}
