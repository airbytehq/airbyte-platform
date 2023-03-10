/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduling;

import io.micronaut.scheduling.TaskExecutors;

/**
 * Names of common task schedulers used to offload work in a Micronaut application.
 */
@SuppressWarnings("PMD.ConstantsInInterface")
public interface AirbyteTaskExecutors extends TaskExecutors {

  /**
   * The name of the {@link java.util.concurrent.ExecutorService} used to schedule health check tasks.
   */
  String HEALTH = "health";

  /**
   * The name of the {@link java.util.concurrent.ExecutorService} used for endpoints that interact
   * with the scheduler.
   */
  String SCHEDULER = "scheduler";

}
