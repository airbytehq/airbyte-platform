/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.constants.AirbyteSecretConstants.AIRBYTE_SECRET_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
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
  private static final String PROPERTIES = "properties";
  private static final String CREDENTIALS = "credentials";

  @Test
  void testGetOAuthConfigPaths() throws IOException, JsonValidationException {
    final ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    final Map<String, List<String>> result = OAuthSecretHelper.getOAuthConfigPaths(connectorSpecification);
    final Map<String, List<String>> expected = Map.of(
        REFRESH_TOKEN, List.of(REFRESH_TOKEN),
        CLIENT_ID, List.of(CLIENT_ID),
        CLIENT_SECRET, List.of(CLIENT_SECRET));
    assertEquals(expected, result);
  }

  @Test
  void testGetOAuthInputPathsForNestedAdvancedAuth() throws IOException, JsonValidationException {
    final ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateNestedAdvancedAuthConnectorSpecification();
    final Map<String, List<String>> result = OAuthSecretHelper.getOAuthInputPaths(connectorSpecification);
    final Map<String, List<String>> expected = Map.of(
        CLIENT_ID, List.of(CREDENTIALS, CLIENT_ID),
        CLIENT_SECRET, List.of(CREDENTIALS, CLIENT_SECRET));
    assertEquals(expected, result);
  }

  @Test
  void testGetOAuthInputPathsForAdvancedAuth() throws IOException, JsonValidationException {
    final ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    final Map<String, List<String>> result = OAuthSecretHelper.getOAuthInputPaths(connectorSpecification);
    final Map<String, List<String>> expected = Map.of(
        CLIENT_ID, List.of(CLIENT_ID),
        CLIENT_SECRET, List.of(CLIENT_SECRET));
    assertEquals(expected, result);
  }

  @Test
  void testSetSecretsInConnectionConfigurationForAdvancedAuth() throws IOException, JsonValidationException {
    final ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification();
    final ObjectNode connectionConfiguration = JsonNodeFactory.instance.objectNode();
    final JsonNode hydratedSecret = Jsons.jsonNode(Map.of(
        REFRESH_TOKEN, EXAMPLE_REFRESH_TOKEN,
        CLIENT_ID, EXAMPLE_CLIENT_ID,
        CLIENT_SECRET, EXAMPLE_CLIENT_SECRET));
    final JsonNode newConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(connectorSpecification, hydratedSecret, connectionConfiguration);

    // Test hydrating empty object
    final ObjectNode expectedConnectionConfiguration = JsonNodeFactory.instance.objectNode();
    expectedConnectionConfiguration.put(REFRESH_TOKEN, EXAMPLE_REFRESH_TOKEN);
    expectedConnectionConfiguration.put(CLIENT_ID, EXAMPLE_CLIENT_ID);
    expectedConnectionConfiguration.put(CLIENT_SECRET, EXAMPLE_CLIENT_SECRET);

    assertEquals(newConnectionConfiguration, expectedConnectionConfiguration);

    // Test overwriting in case users put gibberish values in
    connectionConfiguration.put(REFRESH_TOKEN, EXAMPLE_BAD_REFRESH_TOKEN);
    connectionConfiguration.put(CLIENT_ID, EXAMPLE_BAD_CLIENT_ID);
    connectionConfiguration.put(CLIENT_SECRET, EXAMPLE_BAD_CLIENT_SECRET);

    final JsonNode replacementConnectionConfiguration =
        OAuthSecretHelper.setSecretsInConnectionConfiguration(connectorSpecification, hydratedSecret, connectionConfiguration);

    assertEquals(replacementConnectionConfiguration, expectedConnectionConfiguration);
  }

  @Test
  void testValidateOauthParamConfigAndReturnAdvancedAuthSecretSpec() throws IOException, JsonValidationException {
    final ConnectorSpecification emptyConnectorSpecification = new ConnectorSpecification();
    assertThrows(BadObjectSchemaKnownException.class,
        () -> OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(emptyConnectorSpecification,
            Jsons.jsonNode(Collections.emptyMap())));

    final ConnectorSpecification connectorSpecification = ConnectorSpecificationHelpers.generateNestedAdvancedAuthConnectorSpecification();
    final JsonNode invalidOAuthParamConfig = Jsons.jsonNode(Map.of(
        CLIENT_ID, EXAMPLE_CLIENT_ID,
        CLIENT_SECRET, EXAMPLE_CLIENT_SECRET));

    assertThrows(BadObjectSchemaKnownException.class,
        () -> OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(emptyConnectorSpecification, invalidOAuthParamConfig));

    final JsonNode oneInvalidKeyOAuthParams = Jsons.jsonNode(Map.of(CREDENTIALS, Map.of(
        CLIENT_ID, EXAMPLE_CLIENT_ID)));

    assertThrows(BadObjectSchemaKnownException.class,
        () -> OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(emptyConnectorSpecification, oneInvalidKeyOAuthParams));

    final JsonNode oauthParamConfig = Jsons.jsonNode(Map.of(CREDENTIALS, Map.of(
        CLIENT_ID, EXAMPLE_CLIENT_ID,
        CLIENT_SECRET, EXAMPLE_CLIENT_SECRET)));

    final ConnectorSpecification newConnectorSpecification =
        OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(connectorSpecification, oauthParamConfig);

    final JsonNode expected = Jsons.jsonNode(Map.of(
        PROPERTIES, Map.of(CREDENTIALS,
            Map.of(PROPERTIES, Map.of(CLIENT_ID, airbyteSecretJson(), CLIENT_SECRET, airbyteSecretJson())))));

    assertEquals(newConnectorSpecification.getConnectionSpecification(), expected);
  }

  private static JsonNode airbyteSecretJson() {
    return Jsons.jsonNode(Map.of(AIRBYTE_SECRET_FIELD, true));
  }

}
