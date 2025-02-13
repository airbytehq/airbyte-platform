/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.config;

import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs.AirbyteEdition;
import io.airbyte.config.Configs.DeploymentMode;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.exceptions.DisabledBeanException;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

/**
 * Bean Factory for common Airbyte Configuration.
 */
@Factory
public class AirbyteConfigurationBeanFactory {

  public static <T> T convertToEnum(final String value, final Function<String, T> creatorFunction, final T defaultValue) {
    return StringUtils.isNotEmpty(value) ? creatorFunction.apply(value.toUpperCase(Locale.ROOT)) : defaultValue;
  }

  @Singleton
  public AirbyteVersion airbyteVersion(@Value("${airbyte.version}") final String airbyteVersion) {
    return new AirbyteVersion(airbyteVersion);
  }

  @Singleton
  public DeploymentMode deploymentMode(@Value("${airbyte.deployment-mode:OSS}") final String deploymentMode) {
    return convertToEnum(deploymentMode, DeploymentMode::valueOf, DeploymentMode.OSS);
  }

  /**
   * Fetch the configured edition of the Airbyte instance. Defaults to COMMUNITY.
   */
  @Singleton
  public AirbyteEdition airbyteEdition(@Value("${airbyte.edition:COMMUNITY}") final String airbyteEdition) {
    return convertToEnum(airbyteEdition.toUpperCase(), AirbyteEdition::valueOf, AirbyteEdition.COMMUNITY);
  }

  /**
   * This method provides the airbyte url by preferring the `airbyte.airbyte-url` property over the
   * deprecated `airbyte-yml.webapp-url` property. For backwards compatibility, if
   * `airbyte-yml.airbyte-url` is not provided, this method falls back on `airbyte-yml.webapp-url`.
   */
  @Singleton
  @Named("airbyteUrl")
  public String airbyteUrl(@Value("${airbyte.airbyte-url}") final Optional<String> airbyteUrl,
                           @Value("${airbyte-yml.webapp-url}") final Optional<String> webappUrl) {
    return airbyteUrl.filter(StringUtils::isNotEmpty)
        .orElseGet(() -> webappUrl.orElseThrow(() -> new DisabledBeanException("Airbyte URL not provided.")));
  }

}
