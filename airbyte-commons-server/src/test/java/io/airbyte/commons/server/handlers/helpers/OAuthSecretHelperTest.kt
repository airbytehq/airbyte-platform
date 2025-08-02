/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.getOAuthConfigPaths
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.getOAuthInputPaths
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.setSecretsInConnectionConfiguration
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.io.IOException

internal class OAuthSecretHelperTest {
  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testGetOAuthConfigPaths() {
    val connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification()!!
    val result = getOAuthConfigPaths(connectorSpecification)
    val expected =
      mapOf(
        REFRESH_TOKEN to listOf(REFRESH_TOKEN),
        CLIENT_ID to listOf(CLIENT_ID),
        CLIENT_SECRET to listOf(CLIENT_SECRET),
      )
    Assertions.assertEquals(expected, result)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testGetOAuthInputPathsForNestedAdvancedAuth() {
    val connectorSpecification = ConnectorSpecificationHelpers.generateNestedAdvancedAuthConnectorSpecification()!!
    val result = getOAuthInputPaths(connectorSpecification)
    val expected =
      mapOf(
        CLIENT_ID to listOf(CREDENTIALS, CLIENT_ID),
        CLIENT_SECRET to listOf(CREDENTIALS, CLIENT_SECRET),
      )
    Assertions.assertEquals(expected, result)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testGetOAuthInputPathsForAdvancedAuth() {
    val connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification()!!
    val result = getOAuthInputPaths(connectorSpecification)
    val expected =
      mapOf(
        CLIENT_ID to listOf(CLIENT_ID),
        CLIENT_SECRET to listOf(CLIENT_SECRET),
      )
    Assertions.assertEquals(expected, result)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testSetSecretsInConnectionConfigurationForAdvancedAuth() {
    val connectorSpecification = ConnectorSpecificationHelpers.generateAdvancedAuthConnectorSpecification()!!
    val connectionConfiguration = JsonNodeFactory.instance.objectNode()
    val hydratedSecret =
      jsonNode(
        mapOf(
          REFRESH_TOKEN to EXAMPLE_REFRESH_TOKEN,
          CLIENT_ID to EXAMPLE_CLIENT_ID,
          CLIENT_SECRET to EXAMPLE_CLIENT_SECRET,
        ),
      )
    val newConnectionConfiguration =
      setSecretsInConnectionConfiguration(connectorSpecification, hydratedSecret, connectionConfiguration)

    // Test hydrating empty object
    val expectedConnectionConfiguration = JsonNodeFactory.instance.objectNode()
    expectedConnectionConfiguration.put(REFRESH_TOKEN, EXAMPLE_REFRESH_TOKEN)
    expectedConnectionConfiguration.put(CLIENT_ID, EXAMPLE_CLIENT_ID)
    expectedConnectionConfiguration.put(CLIENT_SECRET, EXAMPLE_CLIENT_SECRET)

    Assertions.assertEquals(newConnectionConfiguration, expectedConnectionConfiguration)

    // Test overwriting in case users put gibberish values in
    connectionConfiguration.put(REFRESH_TOKEN, EXAMPLE_BAD_REFRESH_TOKEN)
    connectionConfiguration.put(CLIENT_ID, EXAMPLE_BAD_CLIENT_ID)
    connectionConfiguration.put(CLIENT_SECRET, EXAMPLE_BAD_CLIENT_SECRET)

    val replacementConnectionConfiguration =
      setSecretsInConnectionConfiguration(connectorSpecification, hydratedSecret, connectionConfiguration)

    Assertions.assertEquals(replacementConnectionConfiguration, expectedConnectionConfiguration)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testValidateOauthParamConfigAndReturnAdvancedAuthSecretSpec() {
    val emptyConnectorSpecification = ConnectorSpecification()
    Assertions.assertThrows<BadObjectSchemaKnownException?>(
      BadObjectSchemaKnownException::class.java,
      Executable {
        validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(
          emptyConnectorSpecification,
          jsonNode(mutableMapOf<Any?, Any?>()),
        )
      },
    )

    val connectorSpecification = ConnectorSpecificationHelpers.generateNestedAdvancedAuthConnectorSpecification()!!
    val invalidOAuthParamConfig =
      jsonNode(
        mapOf(
          CLIENT_ID to EXAMPLE_CLIENT_ID,
          CLIENT_SECRET to EXAMPLE_CLIENT_SECRET,
        ),
      )

    Assertions.assertThrows<BadObjectSchemaKnownException?>(
      BadObjectSchemaKnownException::class.java,
      Executable { validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(emptyConnectorSpecification, invalidOAuthParamConfig) },
    )

    val oneInvalidKeyOAuthParams =
      jsonNode(
        mapOf(
          CREDENTIALS to
            mapOf(
              CLIENT_ID to EXAMPLE_CLIENT_ID,
            ),
        ),
      )

    Assertions.assertThrows<BadObjectSchemaKnownException?>(
      BadObjectSchemaKnownException::class.java,
      Executable { validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(emptyConnectorSpecification, oneInvalidKeyOAuthParams) },
    )

    val oauthParamConfig =
      jsonNode(
        mapOf(
          CREDENTIALS to
            mapOf(
              CLIENT_ID to EXAMPLE_CLIENT_ID,
              CLIENT_SECRET to EXAMPLE_CLIENT_SECRET,
            ),
        ),
      )

    val newConnectorSpecification =
      validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(connectorSpecification, oauthParamConfig)

    val expected =
      jsonNode(
        mapOf(
          PROPERTIES to
            mapOf(
              CREDENTIALS to
                mapOf(
                  PROPERTIES to
                    mapOf(
                      CLIENT_ID to airbyteSecretJson(),
                      CLIENT_SECRET to airbyteSecretJson(),
                    ),
                ),
            ),
        ),
      )

    Assertions.assertEquals(newConnectorSpecification.getConnectionSpecification(), expected)
  }

  companion object {
    const val REFRESH_TOKEN: String = "refresh_token"
    const val CLIENT_ID: String = "client_id"
    const val CLIENT_SECRET: String = "client_secret"
    const val EXAMPLE_REFRESH_TOKEN: String = "so-refreshing"
    const val EXAMPLE_CLIENT_ID: String = "abcd1234"
    const val EXAMPLE_CLIENT_SECRET: String = "shhhh"

    const val EXAMPLE_BAD_REFRESH_TOKEN: String = "not-refreshing"
    const val EXAMPLE_BAD_CLIENT_ID: String = "efgh5678"
    const val EXAMPLE_BAD_CLIENT_SECRET: String = "boom"
    private const val PROPERTIES = "properties"
    private const val CREDENTIALS = "credentials"
    private const val AIRBYTE_SECRET_FIELD = "airbyte_secret"

    private fun airbyteSecretJson(): JsonNode = jsonNode(mapOf(AIRBYTE_SECRET_FIELD to true))
  }
}
