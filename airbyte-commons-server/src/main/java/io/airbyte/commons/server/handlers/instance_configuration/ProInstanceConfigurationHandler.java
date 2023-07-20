/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.instance_configuration;

import io.airbyte.api.model.generated.AuthConfiguration;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.LicenseTypeEnum;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Pro-specific version of the InstanceConfigurationHandler that includes license and auth
 * configuration details for the instance.
 */
@Slf4j
@Singleton
@RequiresAirbyteProEnabled
@Replaces(DefaultInstanceConfigurationHandler.class)
public class ProInstanceConfigurationHandler implements InstanceConfigurationHandler {

  private final AirbyteKeycloakConfiguration keycloakConfiguration;
  private final ActiveAirbyteLicense activeAirbyteLicense;
  private final String webappUrl;

  public ProInstanceConfigurationHandler(final AirbyteKeycloakConfiguration keycloakConfiguration,
                                         final ActiveAirbyteLicense activeAirbyteLicense,
                                         @Value("${airbyte.webapp-url}") final String webappUrl) {
    this.keycloakConfiguration = keycloakConfiguration;
    this.activeAirbyteLicense = activeAirbyteLicense;
    this.webappUrl = webappUrl;
  }

  @Override
  public InstanceConfigurationResponse getInstanceConfiguration() {
    final LicenseTypeEnum licenseTypeEnum = LicenseTypeEnum.fromValue(activeAirbyteLicense.getLicenseType().getValue());

    return new InstanceConfigurationResponse()
        .edition(InstanceConfigurationResponse.EditionEnum.PRO)
        .licenseType(licenseTypeEnum)
        .auth(getAuthConfiguration())
        .webappUrl(webappUrl);
  }

  private AuthConfiguration getAuthConfiguration() {
    return new AuthConfiguration()
        .clientId(getWebClientId())
        .defaultRealm(getAirbyteRealm());
  }

  private String getAirbyteRealm() {
    return keycloakConfiguration.getAirbyteRealm();
  }

  private String getWebClientId() {
    return keycloakConfiguration.getWebClientId();
  }

}
