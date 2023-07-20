/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.instance_configuration;

import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Default InstanceConfigurationHandler that returns the default "community" configuration for
 * Airbyte. For Airbyte Pro, this singleton should be replaced with the
 * {@link ProInstanceConfigurationHandler} that returns Pro-specific details.
 */
@Slf4j
@Singleton
public class DefaultInstanceConfigurationHandler implements InstanceConfigurationHandler {

  private final String webappUrl;

  // the injected webapp-url value defaults to `null` to preserve backwards compatibility.
  // TODO remove the default value once configurations are standardized to always include airbyte.yml
  public DefaultInstanceConfigurationHandler(@Value("${airbyte.webapp-url:null}") final String webappUrl) {
    this.webappUrl = webappUrl;
  }

  @Override
  public InstanceConfigurationResponse getInstanceConfiguration() {
    return new InstanceConfigurationResponse()
        .edition(InstanceConfigurationResponse.EditionEnum.COMMUNITY)
        .licenseType(null)
        .auth(null)
        .webappUrl(webappUrl);
  }

}
