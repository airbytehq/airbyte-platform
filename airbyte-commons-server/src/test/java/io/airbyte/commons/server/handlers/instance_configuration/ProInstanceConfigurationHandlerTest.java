/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.instance_configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.api.model.generated.AuthConfiguration;
import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.commons.auth.config.AirbyteKeycloakConfiguration;
import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense;
import io.airbyte.commons.license.AirbyteLicense.LicenseType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProInstanceConfigurationHandlerTest {

  private static final String WEBAPP_URL = "http://localhost:8000";
  private static final String AIRBYTE_REALM = "airbyte";
  private static final String WEB_CLIENT_ID = "airbyte-webapp";

  private ProInstanceConfigurationHandler proInstanceConfigurationHandler;

  @BeforeEach
  void setup() {
    final ActiveAirbyteLicense activeAirbyteLicense = new ActiveAirbyteLicense();
    activeAirbyteLicense.setLicense(new AirbyteLicense(LicenseType.PRO));

    final AirbyteKeycloakConfiguration keycloakConfiguration = new AirbyteKeycloakConfiguration();
    keycloakConfiguration.setAirbyteRealm(AIRBYTE_REALM);
    keycloakConfiguration.setWebClientId(WEB_CLIENT_ID);

    proInstanceConfigurationHandler = new ProInstanceConfigurationHandler(keycloakConfiguration, activeAirbyteLicense, WEBAPP_URL);
  }

  @Test
  void testGetInstanceConfiguration() {
    final InstanceConfigurationResponse expected = new InstanceConfigurationResponse()
        .edition(InstanceConfigurationResponse.EditionEnum.PRO)
        .licenseType(InstanceConfigurationResponse.LicenseTypeEnum.PRO)
        .auth(new AuthConfiguration()
            .clientId(WEB_CLIENT_ID)
            .defaultRealm(AIRBYTE_REALM))
        .webappUrl(WEBAPP_URL);

    final InstanceConfigurationResponse actual = proInstanceConfigurationHandler.getInstanceConfiguration();

    assertEquals(expected, actual);
  }

}
