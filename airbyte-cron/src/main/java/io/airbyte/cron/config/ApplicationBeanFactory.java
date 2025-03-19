/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.config;

import io.airbyte.commons.constants.WorkerConstants.KubeConstants;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import kotlin.jvm.functions.Function1;

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
public class ApplicationBeanFactory {

  @Singleton
  @Named("replicationNotStartedTimeout")
  public Duration notStartedTimeout() {
    final var sourcePodTimeoutMs = KubeConstants.FULL_POD_TIMEOUT;
    final var destPodTimeoutMs = KubeConstants.FULL_POD_TIMEOUT;
    final var orchestratorInitPodTimeoutMs = KubeConstants.INIT_CONTAINER_STARTUP_TIMEOUT;

    return sourcePodTimeoutMs
        .plus(destPodTimeoutMs)
        .plus(orchestratorInitPodTimeoutMs)
        .multipliedBy(12) // Durations methods don't take doubles, so we do this.
        .dividedBy(10); // This is equivalent to multiplying by 1.2
  }

  @Singleton
  public Function1<ZoneId, OffsetDateTime> timeProvider() {
    return OffsetDateTime::now;
  }

}
