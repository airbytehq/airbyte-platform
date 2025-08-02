/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.extractOauthConfigurationPaths
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties
import io.airbyte.protocol.models.v0.OAuthConfigSpecification
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class OAuthHelperTest {
  @Test
  fun testExtract() {
    val input =
      deserialize(
        """
        {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "tenant_id": {
                    "type": "string",
                    "path_in_connector_config": ["tenant_id"]
                  },
                  "another_property": {
                    "type": "string",
                    "path_in_connector_config": ["another", "property"]
                  }
                }
              }
        
        """.trimIndent(),
      )

    val expected =
      mapOf(
        "tenant_id" to mutableListOf("tenant_id"),
        "another_property" to mutableListOf("another", "property"),
      )

    Assertions
      .assertThat(extractOauthConfigurationPaths(input))
      .containsExactlyInAnyOrderEntriesOf(expected)
  }

  @Test
  fun testUpdateOauthConfigToAcceptAdditionalUserInputProperties() {
    val input: OAuthConfigSpecification =
      deserialize(
        """
        {
              "complete_oauth_output_specification": {},
              "complete_oauth_server_input_specification": {},
              "complete_oauth_server_output_specification": {},
              "oauth_connector_input_specification": {},
              "oauth_user_input_from_connector_config_specification": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "subdomain": {
                    "type": "string",
                    "path_in_connector_config": ["credentials", "subdomain"]
                  }
                }
              }
            }
        
        """.trimIndent(),
        OAuthConfigSpecification::class.java,
      )

    val expected: OAuthConfigSpecification =
      deserialize(
        """
        {
              "complete_oauth_output_specification": {},
              "complete_oauth_server_input_specification": {},
              "complete_oauth_server_output_specification": {},
              "oauth_connector_input_specification": {},
              "oauth_user_input_from_connector_config_specification": {
                "type": "object",
                "additionalProperties": true,
                "properties": {
                  "subdomain": {
                    "type": "string",
                    "path_in_connector_config": ["credentials", "subdomain"]
                  }
                }
              }
            }
        
        """.trimIndent(),
        OAuthConfigSpecification::class.java,
      )
    updateOauthConfigToAcceptAdditionalUserInputProperties(input)
    Assertions.assertThat(input).isEqualTo(expected)
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "{\"oauth_user_input_from_connector_config_specification\": {}}", "{\"oauth_user_input_from_connector_config_specification\": null}", "{}",
    ],
  )
  fun testUpdateOauthConfigToAcceptAdditionalUserInputPropertiesHandlesEdgeCases(jsonStringConfig: String) {
    val input: OAuthConfigSpecification =
      deserialize(
        jsonStringConfig,
        OAuthConfigSpecification::class.java,
      )

    val expected: OAuthConfigSpecification =
      deserialize(
        """
        {
            "oauth_user_input_from_connector_config_specification": {
                "type": "object",
                "additionalProperties": true
            }
        }
        
        """.trimIndent(),
        OAuthConfigSpecification::class.java,
      )

    updateOauthConfigToAcceptAdditionalUserInputProperties(input)

    Assertions.assertThat(input).isEqualTo(expected)
  }
}
