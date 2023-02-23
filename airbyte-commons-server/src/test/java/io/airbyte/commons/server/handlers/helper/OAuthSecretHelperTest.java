/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OAuthSecretHelperTest {

  @Test
  void testGetOAuthConfigPaths() throws IOException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    OAuthSecretHelper.getOAuthConfigPaths(connectorSpecification);
  }

  @Test
  void testBuildKeyToPathInConnectorConfigMap() throws IOException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    Map<String, List<String>> result = OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(connectorSpecification.getConnectionSpecification());
    Map<String, List<String>> expected = Map.of(
        "refresh_token", List.of("refresh_token"),
        "client_id", List.of("client_id"),
        "client_secret", List.of("client_secret"));
    assertEquals(expected, result);
  }

  @Test
  void testSetSecretsInConnectionConfiguration() {

  }

}
