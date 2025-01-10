/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AdvancedAuth.AuthFlowTypeEnum;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.AdvancedAuth;
import io.airbyte.protocol.models.AdvancedAuth.AuthFlowType;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OauthModelConverterTest {

  private static Stream<Arguments> testProvider() {
    return Stream.of(
        // oauth 1.0 with non-nested fields
        Arguments.of(
            AuthFlowType.OAUTH_1_0,
            List.of("auth_type"),
            "OAuth",
            Jsons.deserialize("{\"app_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"app_id\"]}}"),
            Jsons.deserialize("{\"refresh_token\": {\"type\": \"string\", \"path_in_connector_config\": [\"refresh_token\"]}}"),
            Jsons.deserialize("{\"client_id\": {\"type\": \"string\"}, \"client_secret\": {\"type\": \"string\"}}"),
            Jsons.deserialize("{\"client_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"client_id\"]},"
                + "\"client_secret\": {\"type\": \"string\", \"path_in_connector_config\": [\"client_secret\"]}}")),
        // oauth 2.0 with nested fields
        Arguments.of(
            AuthFlowType.OAUTH_2_0,
            List.of("credentials", "auth_type"),
            "OAuth",
            Jsons.deserialize("{\"app_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"app_id\"]}}"),
            Jsons.deserialize("{\"refresh_token\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"refresh_token\"]}}"),
            Jsons.deserialize("{\"client_id\": {\"type\": \"string\"}, \"client_secret\": {\"type\": \"string\"}}"),
            Jsons.deserialize("{\"client_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"client_id\"]},"
                + "\"client_secret\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"client_secret\"]}}")));
  }

  @ParameterizedTest
  @MethodSource("testProvider")
  void testIt(AuthFlowType authFlowType,
              List<String> predicateKey,
              String predicateValue,
              JsonNode oauthUserInputFromConnectorConfigSpecification,
              JsonNode completeOauthOutputSpecification,
              JsonNode completeOauthServerInputSpecification,
              JsonNode completeOauthServerOutputSpecification) {
    final ConnectorSpecification input = new ConnectorSpecification().withAdvancedAuth(
        new AdvancedAuth()
            .withAuthFlowType(authFlowType)
            .withPredicateKey(predicateKey)
            .withPredicateValue(predicateValue)
            .withOauthConfigSpecification(
                new OAuthConfigSpecification()
                    .withOauthUserInputFromConnectorConfigSpecification(oauthUserInputFromConnectorConfigSpecification)
                    .withCompleteOauthOutputSpecification(completeOauthOutputSpecification)
                    .withCompleteOauthServerInputSpecification(completeOauthServerInputSpecification)
                    .withCompleteOauthServerOutputSpecification(completeOauthServerOutputSpecification)));

    final AuthFlowTypeEnum expectedAuthFlowTypeEnum =
        authFlowType.equals(AuthFlowType.OAUTH_1_0) ? AuthFlowTypeEnum.OAUTH1_0 : AuthFlowTypeEnum.OAUTH2_0;

    final io.airbyte.api.model.generated.AdvancedAuth expected = new io.airbyte.api.model.generated.AdvancedAuth()
        .authFlowType(expectedAuthFlowTypeEnum)
        .predicateKey(predicateKey)
        .predicateValue(predicateValue)
        .oauthConfigSpecification(
            new io.airbyte.api.model.generated.OAuthConfigSpecification()
                .oauthUserInputFromConnectorConfigSpecification(oauthUserInputFromConnectorConfigSpecification)
                .completeOAuthOutputSpecification(completeOauthOutputSpecification)
                .completeOAuthServerInputSpecification(completeOauthServerInputSpecification)
                .completeOAuthServerOutputSpecification(completeOauthServerOutputSpecification));

    final Optional<io.airbyte.api.model.generated.AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(input);
    assertTrue(advancedAuth.isPresent());
    assertEquals(expected, advancedAuth.get());
  }

}
