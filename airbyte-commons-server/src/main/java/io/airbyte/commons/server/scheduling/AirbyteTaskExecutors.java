/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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

  /**
   * The name of the {@link java.util.concurrent.ExecutorService} used for endpoints that belong to
   * the public API.
   */
  String PUBLIC_API = "public-api";

  /**
   * The name of the {@link java.util.concurrent.ExecutorService} used for webhook endpoints that are
   * called by external systems.
   */
  String WEBHOOK = "webhook";

}
