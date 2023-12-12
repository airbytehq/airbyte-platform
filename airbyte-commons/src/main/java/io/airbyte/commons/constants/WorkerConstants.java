/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants;

import java.time.Duration;

/**
 * Worker constants.
 */
public class WorkerConstants {

  public static final String SOURCE_CONFIG_JSON_FILENAME = "source_config.json";
  public static final String DESTINATION_CONFIG_JSON_FILENAME = "destination_config.json";

  public static final String SOURCE_CATALOG_JSON_FILENAME = "source_catalog.json";
  public static final String DESTINATION_CATALOG_JSON_FILENAME = "destination_catalog.json";
  public static final String INPUT_STATE_JSON_FILENAME = "input_state.json";

  public static final String RESET_JOB_SOURCE_DOCKER_IMAGE_STUB = "airbyte_empty";

  public static final String WORKER_ENVIRONMENT = "WORKER_ENVIRONMENT";

  public static final String DD_ENV_VAR = "-XX:+ExitOnOutOfMemoryError "
      + "-XX:MaxRAMPercentage=75.0 "
      + "-javaagent:/airbyte/dd-java-agent.jar "
      + "-Ddd.profiling.enabled=true "
      + "-XX:FlightRecorderOptions=stackdepth=256 "
      + "-Ddd.trace.sample.rate=0.5 "
      + "-Ddd.trace.request_header.tags=User-Agent:http.useragent";

  public static class KubeConstants {

    public static final Duration INIT_CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(5);
    public static final Duration INIT_CONTAINER_TERMINATION_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration POD_READY_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration FULL_POD_TIMEOUT = INIT_CONTAINER_STARTUP_TIMEOUT.plus(INIT_CONTAINER_TERMINATION_TIMEOUT).plus(POD_READY_TIMEOUT);

  }

}
