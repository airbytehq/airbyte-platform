/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.instance_configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.api.model.generated.InstanceConfigurationResponse;
import io.airbyte.api.model.generated.InstanceConfigurationResponse.EditionEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultInstanceConfigurationHandlerTest {

  private static final String WEBAPP_URL = "http://localhost:8000";

  private DefaultInstanceConfigurationHandler defaultInstanceConfigurationHandler;

  @BeforeEach
  void setup() {
    defaultInstanceConfigurationHandler = new DefaultInstanceConfigurationHandler(WEBAPP_URL);
  }

  @Test
  void testGetInstanceConfiguration() {
    final InstanceConfigurationResponse expected = new InstanceConfigurationResponse()
        .edition(EditionEnum.COMMUNITY)
        .licenseType(null)
        .auth(null)
        .webappUrl(WEBAPP_URL);

    final InstanceConfigurationResponse actual = defaultInstanceConfigurationHandler.getInstanceConfiguration();

    assertEquals(expected, actual);
  }

}
