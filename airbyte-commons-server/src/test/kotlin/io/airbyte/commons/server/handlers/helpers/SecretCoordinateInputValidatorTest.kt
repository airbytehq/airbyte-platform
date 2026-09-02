/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.SecretCoordinateInputValidator.validateSecretCoordinateInput
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.Companion.fromFullCoordinate
import io.airbyte.config.secrets.SecretCoordinate.ExternalSecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

internal class SecretCoordinateInputValidatorTest {
  private val spec =
    Jsons.deserialize(
      """
      {
        "type": "object",
        "properties": {
          "host": { "type": "string" },
          "password": { "type": "string", "airbyte_secret": true },
          "tunnel": {
            "type": "object",
            "properties": {
              "key": { "type": "string", "airbyte_secret": true }
            }
          },
          "accounts": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "token": { "type": "string", "airbyte_secret": true }
              }
            }
          }
        }
      }
      """.trimIndent(),
    )

  private val configuredStorageId = SecretStorageId(UUID.randomUUID())

  private val airbyteManagedCoordinate =
    AirbyteManagedSecretCoordinate(
      "workspace_",
      UUID.randomUUID(),
      1L,
    ).fullCoordinate

  private val otherAirbyteManagedCoordinate =
    AirbyteManagedSecretCoordinate(
      "workspace_",
      UUID.randomUUID(),
      1L,
    ).fullCoordinate

  private fun config(vararg overrides: Pair<String, String>): JsonNode {
    val values = overrides.toMap()
    return Jsons.deserialize(
      """
      {
        "host": "example.com",
        "password": "${values["password"] ?: "s3cr3t"}",
        "tunnel": { "key": "${values["tunnel.key"] ?: "raw-key"}" },
        "accounts": [
          { "token": "${values["accounts.0.token"] ?: "raw-token-0"}" },
          { "token": "${values["accounts.1.token"] ?: "raw-token-1"}" }
        ]
      }
      """.trimIndent(),
    )
  }

  /**
   * Builds a config holding the given secret nodes at the given config paths, as the update path
   * does when it copies persisted secret nodes over incoming masked values.
   */
  private fun configWithNodes(vararg nodesByPath: Pair<String, Map<String, String>>): JsonNode {
    var withNodes: JsonNode = config()
    nodesByPath.forEach { (path, node) ->
      withNodes = JsonPaths.replaceAtJsonNodeLoud(withNodes, path, Jsons.jsonNode(node))
    }
    return withNodes
  }

  /**
   * Builds the stored state of an actor holding [coordinates] at the given config paths, as
   * [ConfigWithSecretReferences] does: a secret node at each path plus the resolved reference.
   */
  private fun persistedConfig(coordinates: Map<String, SecretCoordinate>): ConfigWithSecretReferences {
    var storedConfig: JsonNode = config()
    coordinates.forEach { (path, coordinate) ->
      storedConfig =
        JsonPaths.replaceAtJsonNodeLoud(storedConfig, path, Jsons.jsonNode(mapOf("_secret" to coordinate.fullCoordinate)))
    }
    return ConfigWithSecretReferences(storedConfig, coordinates.mapValues { (_, coordinate) -> SecretReferenceConfig(coordinate) })
  }

  /**
   * Defaults [persistedCoordinatesByPath] to empty, i.e. the create case where the actor holds no
   * prior coordinate to match against.
   */
  private fun validate(
    config: JsonNode,
    secretStorageId: SecretStorageId?,
    persistedCoordinatesByPath: Map<String, SecretCoordinate> = emptyMap(),
  ) = validateSecretCoordinateInput(config, spec, secretStorageId) {
    persistedCoordinatesByPath.takeIf { it.isNotEmpty() }?.let { persistedConfig(it) }
  }

  @Test
  fun `accepts raw secret values`() {
    assertThatCode { validate(config(), configuredStorageId) }.doesNotThrowAnyException()
  }

  @Test
  fun `accepts the secrets mask`() {
    val masked = config("password" to AirbyteSecretConstants.SECRETS_MASK)
    assertThatCode { validate(masked, configuredStorageId) }.doesNotThrowAnyException()
    assertThatCode { validate(masked, null) }.doesNotThrowAnyException()
  }

  @Test
  fun `accepts an empty config`() {
    assertThatCode { validate(Jsons.emptyObject(), configuredStorageId) }.doesNotThrowAnyException()
  }

  @Test
  fun `rejects an airbyte managed coordinate even with configured storage`() {
    assertThatThrownBy {
      validate(config("password" to prefixed(airbyteManagedCoordinate)), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
      .hasMessage(EXPECTED_MESSAGE)
  }

  @Test
  fun `rejects an airbyte managed coordinate in a nested secret field`() {
    assertThatThrownBy {
      validate(config("tunnel.key" to prefixed(airbyteManagedCoordinate)), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects an airbyte managed coordinate in an array element`() {
    assertThatThrownBy {
      validate(config("accounts.1.token" to prefixed(airbyteManagedCoordinate)), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `accepts an external coordinate for a workspace with configured storage`() {
    assertThatCode {
      validate(config("password" to prefixed("my-vault-secret")), configuredStorageId)
    }.doesNotThrowAnyException()
  }

  @Test
  fun `rejects an external coordinate when the workspace has no secret storage`() {
    assertThatThrownBy {
      validate(config("password" to prefixed("my-vault-secret")), null)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects an external coordinate on the default secret storage`() {
    assertThatThrownBy {
      validate(config("password" to prefixed("my-vault-secret")), SecretStorage.DEFAULT_SECRET_STORAGE_ID)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `accepts an external coordinate unchanged at the same path on the default secret storage`() {
    assertThatCode {
      validate(
        config("password" to prefixed("my-vault-secret")),
        SecretStorage.DEFAULT_SECRET_STORAGE_ID,
        mapOf("\$.password" to ExternalSecretCoordinate("my-vault-secret")),
      )
    }.doesNotThrowAnyException()
  }

  @Test
  fun `rejects a blank coordinate even with configured storage`() {
    assertThatThrownBy {
      validate(config("password" to prefixed("  ")), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects a blank coordinate even when the same path is persisted blank`() {
    assertThatThrownBy {
      validate(
        config("password" to prefixed("")),
        configuredStorageId,
        mapOf("\$.password" to ExternalSecretCoordinate("")),
      )
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `carries over the persisted secret node for a coordinate unchanged at the same path`() {
    val validated =
      validate(
        config("password" to prefixed(airbyteManagedCoordinate)),
        configuredStorageId,
        mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)),
      )

    assertThat(validated["password"]).isEqualTo(Jsons.jsonNode(mapOf("_secret" to airbyteManagedCoordinate)))
  }

  @Test
  fun `leaves a config without prefixed coordinates untouched`() {
    val raw = config()

    assertThat(validate(raw, configuredStorageId)).isEqualTo(raw)
  }

  @Test
  fun `carries over persisted secret nodes at unchanged nested and array paths`() {
    val validated =
      validate(
        config(
          "tunnel.key" to prefixed(airbyteManagedCoordinate),
          "accounts.1.token" to prefixed(otherAirbyteManagedCoordinate),
        ),
        configuredStorageId,
        mapOf(
          "\$.tunnel.key" to fromFullCoordinate(airbyteManagedCoordinate),
          "\$.accounts[1].token" to fromFullCoordinate(otherAirbyteManagedCoordinate),
        ),
      )

    assertThat(validated["tunnel"]["key"]).isEqualTo(Jsons.jsonNode(mapOf("_secret" to airbyteManagedCoordinate)))
    assertThat(validated["accounts"][1]["token"]).isEqualTo(Jsons.jsonNode(mapOf("_secret" to otherAirbyteManagedCoordinate)))
    assertThat(validated["accounts"][0]["token"].asText()).isEqualTo("raw-token-0")
  }

  @Test
  fun `accepts a persisted coordinate for a workspace without configured storage`() {
    assertThatCode {
      validate(
        config("password" to prefixed(airbyteManagedCoordinate)),
        null,
        mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)),
      )
    }.doesNotThrowAnyException()
  }

  @Test
  fun `rejects an airbyte managed coordinate that differs from the one persisted at the same path`() {
    assertThatThrownBy {
      validate(
        config("password" to prefixed(otherAirbyteManagedCoordinate)),
        configuredStorageId,
        mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)),
      )
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects an airbyte managed coordinate persisted at a different path`() {
    assertThatThrownBy {
      validate(
        config("password" to prefixed(airbyteManagedCoordinate)),
        configuredStorageId,
        mapOf("\$.tunnel.key" to fromFullCoordinate(airbyteManagedCoordinate)),
      )
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects an airbyte managed coordinate when only another path matches`() {
    assertThatThrownBy {
      validate(
        config(
          "password" to prefixed(airbyteManagedCoordinate),
          "tunnel.key" to prefixed(otherAirbyteManagedCoordinate),
        ),
        configuredStorageId,
        mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)),
      )
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `leaves a coordinate node unchanged at the same path untouched`() {
    val roundTripped = configWithNodes("\$.password" to mapOf("_secret" to airbyteManagedCoordinate))

    val validated =
      validate(roundTripped, configuredStorageId, mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)))

    assertThat(validated).isEqualTo(roundTripped)
  }

  @Test
  fun `rejects an airbyte managed coordinate node on create`() {
    assertThatThrownBy {
      validate(configWithNodes("\$.password" to mapOf("_secret" to airbyteManagedCoordinate)), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
      .hasMessage(EXPECTED_MESSAGE)
  }

  @Test
  fun `rejects an airbyte managed coordinate node that differs from the one persisted at the same path`() {
    assertThatThrownBy {
      validate(
        configWithNodes("\$.password" to mapOf("_secret" to otherAirbyteManagedCoordinate)),
        configuredStorageId,
        mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)),
      )
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects airbyte managed coordinate nodes in nested and array secret fields`() {
    assertThatThrownBy {
      validate(configWithNodes("\$.tunnel.key" to mapOf("_secret" to airbyteManagedCoordinate)), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)

    assertThatThrownBy {
      validate(configWithNodes("\$.accounts[1].token" to mapOf("_secret" to airbyteManagedCoordinate)), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects an airbyte managed coordinate node carrying a secret reference id`() {
    val withReferenceId =
      configWithNodes(
        "\$.password" to
          mapOf(
            "_secret" to airbyteManagedCoordinate,
            "_secret_reference_id" to UUID.randomUUID().toString(),
          ),
      )

    assertThatThrownBy { validate(withReferenceId, configuredStorageId) }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `accepts a coordinate node matching the stored config when its reference is orphaned`() {
    val roundTripped = configWithNodes("\$.password" to mapOf("_secret" to airbyteManagedCoordinate))

    assertThatCode {
      validateSecretCoordinateInput(roundTripped, spec, configuredStorageId) {
        ConfigWithSecretReferences(roundTripped, emptyMap())
      }
    }.doesNotThrowAnyException()
  }

  @Test
  fun `rejects a coordinate node naming a blank coordinate`() {
    assertThatThrownBy {
      validate(configWithNodes("\$.password" to mapOf("_secret" to "  ")), configuredStorageId)
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `rejects an airbyte managed coordinate node at a path the spec does not declare secret`() {
    val underAdditionalProperty =
      JsonPaths.replaceAtJsonNodeLoud(config(), "\$.host", Jsons.jsonNode(mapOf("_secret" to airbyteManagedCoordinate)))

    assertThatThrownBy { validate(underAdditionalProperty, configuredStorageId) }
      .isInstanceOf(BadRequestException::class.java)

    assertThatThrownBy {
      validate(underAdditionalProperty, configuredStorageId, mapOf("\$.password" to fromFullCoordinate(airbyteManagedCoordinate)))
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `accepts a coordinate node at an undeclared path that matches the stored config`() {
    val stored = JsonPaths.replaceAtJsonNodeLoud(config(), "\$.host", Jsons.jsonNode(mapOf("_secret" to airbyteManagedCoordinate)))

    assertThatCode {
      validateSecretCoordinateInput(stored, spec, configuredStorageId) { ConfigWithSecretReferences(stored, emptyMap()) }
    }.doesNotThrowAnyException()
  }

  @Test
  fun `rejects a new external coordinate node on the default secret storage`() {
    assertThatThrownBy {
      validate(
        configWithNodes("\$.password" to mapOf("_secret" to "my-vault-secret")),
        SecretStorage.DEFAULT_SECRET_STORAGE_ID,
      )
    }.isInstanceOf(BadRequestException::class.java)
  }

  @Test
  fun `accepts an external coordinate node already stored at the same path on the default secret storage`() {
    val roundTripped = configWithNodes("\$.password" to mapOf("_secret" to "my-vault-secret"))

    assertThatCode {
      validateSecretCoordinateInput(roundTripped, spec, SecretStorage.DEFAULT_SECRET_STORAGE_ID) {
        ConfigWithSecretReferences(roundTripped, emptyMap())
      }
    }.doesNotThrowAnyException()
  }

  companion object {
    private val EXPECTED_MESSAGE =
      "Secret coordinates are read-only pointers and are not accepted as configuration input. " +
        "Send the secret value, or ${AirbyteSecretConstants.SECRETS_MASK} to leave the stored secret unchanged."

    private fun prefixed(coordinate: String) = "secret_coordinate::$coordinate"
  }
}
