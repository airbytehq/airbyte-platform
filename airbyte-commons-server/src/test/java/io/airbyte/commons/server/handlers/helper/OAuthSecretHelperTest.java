/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
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
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    Map<String, List<String>> result = OAuthSecretHelper.getOAuthConfigPaths(connectorSpecification);
    Map<String, List<String>> expected = Map.of(
        "refresh_token", List.of("refresh_token"),
        "client_id", List.of("client_id"),
        "client_secret", List.of("client_secret"));
    assertEquals(expected, result);
  }

  @Test
  void testBuildKeyToPathInConnectorConfigMap() throws IOException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
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
  void testSetSecretsInConnectionConfiguration() throws IOException, JsonValidationException, ConfigNotFoundException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    StandardSourceDefinition sourceDefinition = new StandardSourceDefinition().withSpec(connectorSpecification);
    ObjectNode connectionConfiguration = JsonNodeFactory.instance.objectNode();
    Map<String, Object> hydratedSecret = Map.of("refresh_token", "so-refreshing");
    JsonNode newConnectionConfiguration = OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition, hydratedSecret, connectionConfiguration);

    System.out.println(newConnectionConfiguration);
    // TODO - flesh this test out
    assertEquals(1, 1);
  }

}
