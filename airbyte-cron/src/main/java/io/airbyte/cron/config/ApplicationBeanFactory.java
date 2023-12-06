/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.config;

import io.airbyte.commons.constants.WorkerConstants.KubeConstants;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import kotlin.jvm.functions.Function1;
import lombok.extern.slf4j.Slf4j;

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
@Slf4j
public class ApplicationBeanFactory {

  @Singleton
  public MetricClient metricClient() {
    MetricClientFactory.initialize(MetricEmittingApps.CRON);
    return io.airbyte.metrics.lib.MetricClientFactory.getMetricClient();
  }

  @Singleton
  public AirbyteProtocolVersionRange airbyteProtocolVersionRange(
                                                                 @Value("${airbyte.protocol.min-version}") final String minVersion,
                                                                 @Value("${airbyte.protocol.max-version}") final String maxVersion) {
    return new AirbyteProtocolVersionRange(new Version(minVersion), new Version(maxVersion));
  }

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
