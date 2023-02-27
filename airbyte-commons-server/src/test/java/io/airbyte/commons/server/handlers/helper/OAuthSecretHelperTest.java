/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OAuthSecretHelperTest {

  String REFRESH_TOKEN = "refresh_token";
  String CLIENT_ID = "client_id";
  String CLIENT_SECRET = "client_secret";

  @Test
  void testGetOAuthConfigPaths() throws IOException, JsonValidationException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    Map<String, List<String>> result = OAuthSecretHelper.getOAuthConfigPaths(connectorSpecification);
    Map<String, List<String>> expected = Map.of(
        REFRESH_TOKEN, List.of("refresh_token"),
        CLIENT_ID, List.of("client_id"),
        CLIENT_SECRET, List.of("client_secret"));
    assertEquals(expected, result);
  }

  @Test
  void testBuildKeyToPathInConnectorConfigMap() throws IOException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    Map<String, List<String>> resultForCompleteOauthOutputSpecification = OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(
        connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthOutputSpecification());
    Map<String, List<String>> expectedForCompleteOAuthOutputSpecification = Map.of(
        REFRESH_TOKEN, List.of("refresh_token"));
    assertEquals(expectedForCompleteOAuthOutputSpecification, resultForCompleteOauthOutputSpecification);

    Map<String, List<String>> resultForCompleteOauthServerOutputSpecification = OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(
        connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification());
    Map<String, List<String>> expectedForCompleteOAuthServerOutputSpecification = Map.of(
        CLIENT_ID, List.of("client_id"),
        CLIENT_SECRET, List.of("client_secret"));
    assertEquals(expectedForCompleteOAuthServerOutputSpecification, resultForCompleteOauthServerOutputSpecification);
  }

  @Test
  void testSetSecretsInConnectionConfigurationForAdvancedAuth() throws IOException, JsonValidationException, ConfigNotFoundException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    StandardSourceDefinition sourceDefinition = new StandardSourceDefinition().withSpec(connectorSpecification);
    ObjectNode connectionConfiguration = JsonNodeFactory.instance.objectNode();
    JsonNode hydratedSecret = Jsons.jsonNode(Map.of(
        REFRESH_TOKEN, "so-refreshing",
        CLIENT_ID, "abcd1234",
        CLIENT_SECRET, "shhhh"));
    JsonNode newConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition, hydratedSecret, connectionConfiguration);

    // Test hydrating empty object
    ObjectNode expectedConnectionConfiguration = JsonNodeFactory.instance.objectNode();
    expectedConnectionConfiguration.put(REFRESH_TOKEN, "so-refreshing");
    expectedConnectionConfiguration.put(CLIENT_ID, "abcd1234");
    expectedConnectionConfiguration.put(CLIENT_SECRET, "shhhh");

    assertEquals(newConnectionConfiguration, expectedConnectionConfiguration);

    // Test overwriting in case users put gibberish values in
    connectionConfiguration.put(REFRESH_TOKEN, "not-refreshing");
    connectionConfiguration.put(CLIENT_ID, "efgh5678");
    connectionConfiguration.put(CLIENT_SECRET, "boom");

    JsonNode replacementConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition, hydratedSecret, connectionConfiguration);

    assertEquals(replacementConnectionConfiguration, expectedConnectionConfiguration);
  }

  @Test
  void testSetSecretsInConnectionConfigurationForAuthSpecification() throws IOException, JsonValidationException, ConfigNotFoundException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAuthSpecificationConnectorSpecification();
    StandardSourceDefinition sourceDefinition = new StandardSourceDefinition().withSpec(connectorSpecification);
    ObjectNode connectionConfiguration = JsonNodeFactory.instance.objectNode();
    JsonNode hydratedSecret = Jsons.jsonNode(Map.of("credentials", Map.of(
        REFRESH_TOKEN, "so-refreshing",
        CLIENT_ID, "abcd1234",
        CLIENT_SECRET, "shhhh")));
    JsonNode newConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition, hydratedSecret, connectionConfiguration);

    // Test hydrating empty object
    assertEquals(newConnectionConfiguration, hydratedSecret);

    // Test overwriting in case users put gibberish values in
    ObjectNode credentials = JsonNodeFactory.instance.objectNode();
    credentials.set(REFRESH_TOKEN, TextNode.valueOf("not-refreshing"));
    credentials.set(CLIENT_ID, TextNode.valueOf("efgh5678"));
    credentials.set(CLIENT_SECRET, TextNode.valueOf("boom"));
    connectionConfiguration.set("credentials", credentials);

    JsonNode replacementConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition, hydratedSecret, connectionConfiguration);

    assertEquals(replacementConnectionConfiguration, hydratedSecret);
  }

}
