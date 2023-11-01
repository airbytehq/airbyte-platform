/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.config;

import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
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
  public FeatureFlags featureFlags() {
    return new EnvVariableFeatureFlags();
  }

}
