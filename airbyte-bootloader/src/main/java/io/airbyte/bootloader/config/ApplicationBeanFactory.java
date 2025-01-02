/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.config;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Singleton;

/**
 * Micronaut bean factory for general application-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ApplicationBeanFactory {

  @Singleton
  public AirbyteProtocolVersionRange airbyteProtocolTargetVersionRange(@Value("${airbyte.protocol.target.range.min-version}") final String min,
                                                                       @Value("${airbyte.protocol.target.range.max-version}") final String max) {
    return new AirbyteProtocolVersionRange(new Version(min), new Version(max));
  }

  @Singleton
  public MetricClient metricClient() {
    MetricClientFactory.initialize(MetricEmittingApps.BOOTLOADER);
    return io.airbyte.metrics.lib.MetricClientFactory.getMetricClient();
  }

  @Singleton
  @Requires(env = Environment.KUBERNETES)
  public KubernetesClient kubernetesClient() {
    return new KubernetesClientBuilder().build();
  }

}
