/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.AdvancedAuth.AuthFlowTypeEnum
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.server.converters.OauthModelConverter.getAdvancedAuth
import io.airbyte.protocol.models.v0.AdvancedAuth
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.Optional
import java.util.stream.Stream

internal class OauthModelConverterTest {
  @ParameterizedTest
  @MethodSource("testProvider")
  fun testIt(
    authFlowType: AdvancedAuth.AuthFlowType,
    predicateKey: MutableList<String?>?,
    predicateValue: String?,
    oauthUserInputFromConnectorConfigSpecification: JsonNode?,
    completeOauthOutputSpecification: JsonNode?,
    completeOauthServerInputSpecification: JsonNode?,
    completeOauthServerOutputSpecification: JsonNode?,
  ) {
    val input =
      ConnectorSpecification().withAdvancedAuth(
        AdvancedAuth()
          .withAuthFlowType(authFlowType)
          .withPredicateKey(predicateKey)
          .withPredicateValue(predicateValue)
          .withOauthConfigSpecification(
            OAuthConfigSpecification()
              .withOauthUserInputFromConnectorConfigSpecification(oauthUserInputFromConnectorConfigSpecification)
              .withCompleteOauthOutputSpecification(completeOauthOutputSpecification)
              .withCompleteOauthServerInputSpecification(completeOauthServerInputSpecification)
              .withCompleteOauthServerOutputSpecification(completeOauthServerOutputSpecification),
          ),
      )

    val expectedAuthFlowTypeEnum =
      if (authFlowType == AdvancedAuth.AuthFlowType.OAUTH_1_0) AuthFlowTypeEnum.OAUTH1_0 else AuthFlowTypeEnum.OAUTH2_0

    val expected =
      io.airbyte.api.model.generated
        .AdvancedAuth()
        .authFlowType(expectedAuthFlowTypeEnum)
        .predicateKey(predicateKey)
        .predicateValue(predicateValue)
        .oauthConfigSpecification(
          io.airbyte.api.model.generated
            .OAuthConfigSpecification()
            .oauthUserInputFromConnectorConfigSpecification(oauthUserInputFromConnectorConfigSpecification)
            .completeOAuthOutputSpecification(completeOauthOutputSpecification)
            .completeOAuthServerInputSpecification(completeOauthServerInputSpecification)
            .completeOAuthServerOutputSpecification(completeOauthServerOutputSpecification),
        )

    val advancedAuth: Optional<io.airbyte.api.model.generated.AdvancedAuth> = getAdvancedAuth(input)
    Assertions.assertTrue(advancedAuth.isPresent())
    Assertions.assertEquals(expected, advancedAuth.get())
  }

  companion object {
    @JvmStatic
    private fun testProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>( // oauth 1.0 with non-nested fields
        Arguments.of(
          AdvancedAuth.AuthFlowType.OAUTH_1_0,
          mutableListOf<String?>("auth_type"),
          "OAuth",
          deserialize("{\"app_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"app_id\"]}}"),
          deserialize("{\"refresh_token\": {\"type\": \"string\", \"path_in_connector_config\": [\"refresh_token\"]}}"),
          deserialize("{\"client_id\": {\"type\": \"string\"}, \"client_secret\": {\"type\": \"string\"}}"),
          deserialize(
            "{\"client_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"client_id\"]}," +
              "\"client_secret\": {\"type\": \"string\", \"path_in_connector_config\": [\"client_secret\"]}}",
          ),
        ), // oauth 2.0 with nested fields
        Arguments.of(
          AdvancedAuth.AuthFlowType.OAUTH_2_0,
          mutableListOf<String?>("credentials", "auth_type"),
          "OAuth",
          deserialize("{\"app_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"app_id\"]}}"),
          deserialize("{\"refresh_token\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"refresh_token\"]}}"),
          deserialize("{\"client_id\": {\"type\": \"string\"}, \"client_secret\": {\"type\": \"string\"}}"),
          deserialize(
            "{\"client_id\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"client_id\"]}," +
              "\"client_secret\": {\"type\": \"string\", \"path_in_connector_config\": [\"credentials\", \"client_secret\"]}}",
          ),
        ),
      )
  }
}
