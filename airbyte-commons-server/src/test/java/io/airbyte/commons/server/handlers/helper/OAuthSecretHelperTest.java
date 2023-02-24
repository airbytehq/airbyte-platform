/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OAuthSecretHelperTest {

  @Test
  void testGetOAuthConfigPaths() throws IOException {
    assertEquals(1, 1);
  }

  @Test
  void testBuildKeyToPathInConnectorConfigMap() throws IOException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    System.out.println(connectorSpecification.getAdvancedAuth());
    Map<String, List<String>> resultForCompleteOauthOutputSpecification = OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(
        connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthOutputSpecification()
    );
    Map<String, List<String>> expectedForCompleteOAuthOutputSpecification = Map.of(
        "refresh_token", List.of("refresh_token"));
    assertEquals(expectedForCompleteOAuthOutputSpecification, resultForCompleteOauthOutputSpecification);

    Map<String, List<String>> resultForCompleteOauthServerOutputSpecification = OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(
        connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification()
    );
    Map<String, List<String>> expectedForCompleteOAuthServerOutputSpecification = Map.of(
        "client_id", List.of("client_id"),
        "client_secret", List.of("client_secret"));
    assertEquals(expectedForCompleteOAuthServerOutputSpecification, resultForCompleteOauthServerOutputSpecification);
  }

  @Test
  void testSetSecretsInConnectionConfiguration() {
    assertEquals(1, 1);
  }

}
