/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.config;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.persistence.split_secrets.JsonSecretsProcessor;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Micronaut bean factory for general singletons.
 */
@Factory
@Slf4j
public class ApplicationBeanFactory {

  /**
   * Json secrets process.
   *
   * @return json secrets process
   */
  @Singleton
  public JsonSecretsProcessor jsonSecretsProcessor() {
    return JsonSecretsProcessor.builder()
        .copySecrets(false)
        .build();
  }

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

}
