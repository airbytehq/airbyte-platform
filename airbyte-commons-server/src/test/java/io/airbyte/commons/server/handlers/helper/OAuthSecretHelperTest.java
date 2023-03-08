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

  public static final String REFRESH_TOKEN = "refresh_token";
  public static final String CLIENT_ID = "client_id";
  public static final String CLIENT_SECRET = "client_secret";
  public static final String EXAMPLE_REFRESH_TOKEN = "so-refreshing";
  public static final String EXAMPLE_CLIENT_ID = "abcd1234";
  public static final String EXAMPLE_CLIENT_SECRET = "shhhh";

  public static final String EXAMPLE_BAD_REFRESH_TOKEN = "not-refreshing";
  public static final String EXAMPLE_BAD_CLIENT_ID = "efgh5678";
  public static final String EXAMPLE_BAD_CLIENT_SECRET = "boom";

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
  void testSetSecretsInConnectionConfigurationForAdvancedAuth() throws IOException, JsonValidationException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    StandardSourceDefinition sourceDefinition = new StandardSourceDefinition().withSpec(connectorSpecification);
    ObjectNode connectionConfiguration = JsonNodeFactory.instance.objectNode();
    JsonNode hydratedSecret = Jsons.jsonNode(Map.of(
        REFRESH_TOKEN, EXAMPLE_REFRESH_TOKEN,
        CLIENT_ID, EXAMPLE_CLIENT_ID,
        CLIENT_SECRET, EXAMPLE_CLIENT_SECRET));
    final JsonNode newConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition.getSpec(), hydratedSecret, connectionConfiguration);

    // Test hydrating empty object
    ObjectNode expectedConnectionConfiguration = JsonNodeFactory.instance.objectNode();
    expectedConnectionConfiguration.put(REFRESH_TOKEN, EXAMPLE_REFRESH_TOKEN);
    expectedConnectionConfiguration.put(CLIENT_ID, EXAMPLE_CLIENT_ID);
    expectedConnectionConfiguration.put(CLIENT_SECRET, EXAMPLE_CLIENT_SECRET);

    assertEquals(newConnectionConfiguration, expectedConnectionConfiguration);

    // Test overwriting in case users put gibberish values in
    connectionConfiguration.put(REFRESH_TOKEN, EXAMPLE_BAD_REFRESH_TOKEN);
    connectionConfiguration.put(CLIENT_ID, EXAMPLE_BAD_CLIENT_ID);
    connectionConfiguration.put(CLIENT_SECRET, EXAMPLE_BAD_CLIENT_SECRET);

    JsonNode replacementConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition.getSpec(), hydratedSecret, connectionConfiguration);

    assertEquals(replacementConnectionConfiguration, expectedConnectionConfiguration);
  }

  @Test
  void testSetSecretsInConnectionConfigurationForAuthSpecification() throws IOException, JsonValidationException, ConfigNotFoundException {
    ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAuthSpecificationConnectorSpecification();
    StandardSourceDefinition sourceDefinition = new StandardSourceDefinition().withSpec(connectorSpecification);
    ObjectNode connectionConfiguration = JsonNodeFactory.instance.objectNode();
    JsonNode hydratedSecret = Jsons.jsonNode(Map.of("credentials", Map.of(
        REFRESH_TOKEN, EXAMPLE_REFRESH_TOKEN,
        CLIENT_ID, EXAMPLE_CLIENT_ID,
        CLIENT_SECRET, EXAMPLE_CLIENT_SECRET)));
    JsonNode newConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition.getSpec(), hydratedSecret, connectionConfiguration);

    // Test hydrating empty object
    assertEquals(newConnectionConfiguration, hydratedSecret);

    // Test overwriting in case users put gibberish values in
    ObjectNode credentials = JsonNodeFactory.instance.objectNode();
    credentials.set(REFRESH_TOKEN, TextNode.valueOf(EXAMPLE_BAD_REFRESH_TOKEN));
    credentials.set(CLIENT_ID, TextNode.valueOf(EXAMPLE_BAD_CLIENT_ID));
    credentials.set(CLIENT_SECRET, TextNode.valueOf(EXAMPLE_BAD_CLIENT_SECRET));
    connectionConfiguration.set("credentials", credentials);

    JsonNode replacementConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(sourceDefinition.getSpec(), hydratedSecret, connectionConfiguration);

    assertEquals(replacementConnectionConfiguration, hydratedSecret);
  }

}
