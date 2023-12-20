/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.config;

import io.micrometer.statsd.StatsdMeterRegistry;
import io.micronaut.configuration.metrics.aggregator.MeterRegistryConfigurer;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.core.annotation.Order;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.LinkedHashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Temporarily copy this from the workload-launcher. Ultimately, this will move to
// airbyte-metrics/metrics-lib
// and would provide a mechanism to override/add additional tags and/or define the tags that will be
// included
// in metrics

/**
 * Custom Micronaut {@link MeterRegistryConfigurer} used to ensure that a common set of tags are
 * added to every Micrometer registry. Specifically, this class ensures that the tags for the
 * DataDog environment, service name and deployment version are added to the metrics produced by
 * Micrometer, if those values are in the current environment.
 */
@Order(Integer.MAX_VALUE)
@Singleton
@Named("statsDRegistryConfigurer")
@RequiresMetrics
public class StatsDRegistryConfigurer implements MeterRegistryConfigurer<StatsdMeterRegistry>, Ordered {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatsDRegistryConfigurer.class);

  private static final String DATA_DOG_AGENT_HOST_TAG = "DD_AGENT_HOST";
  private static final String DATA_DOG_ENVIRONMENT_TAG = "DD_ENV";
  private static final String DATA_DOG_SERVICE_TAG = "DD_SERVICE";
  private static final String DATA_DOG_VERSION_TAG = "DD_VERSION";

  @Override
  public void configure(final StatsdMeterRegistry meterRegistry) {
    /*
     * Use a LinkedHashSet to maintain order as items are added to the set. This ensures that the items
     * are output as key1, value1, key2, value2, etc in order to maintain the relationship between key
     * value pairs.
     */
    final Set<String> tags = new LinkedHashSet<>();

    possiblyAddTag(DATA_DOG_ENVIRONMENT_TAG, "env", tags);
    possiblyAddTag(DATA_DOG_AGENT_HOST_TAG, "host", tags);
    possiblyAddTag(DATA_DOG_SERVICE_TAG, "service", tags);
    possiblyAddTag(DATA_DOG_VERSION_TAG, "version", tags);

    LOGGER.debug("Adding common tags to the StatsD Micrometer meter registry configuration: {}", tags);

    meterRegistry.config().commonTags(tags.toArray(new String[] {}));
  }

  @Override
  public Class<StatsdMeterRegistry> getType() {
    return StatsdMeterRegistry.class;
  }

  /**
   * Safely adds the value associated with the provided environment variable if it exists and is not
   * blank.
   *
   * @param envVar The name of the environment variable.
   * @param tagName The name of the tag to add to the set if the environment variable is not blank.
   * @param tags The set of tags.
   */
  private void possiblyAddTag(
                              final String envVar,
                              final String tagName,
                              final Set<String> tags) {
    final String envVarValue = System.getenv(envVar);
    if (StringUtils.isNotEmpty(envVarValue)) {
      tags.add(tagName);
      tags.add(envVarValue);
    }
  }

}
