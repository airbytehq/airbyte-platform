/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.publicApi.server.wireformat

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.publicApi.server.generated.models.ApplicationTokenRequestWithGrant
import io.airbyte.publicApi.server.generated.models.JobStatusEnum
import io.airbyte.publicApi.server.generated.models.PublicAccessTokenResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

internal class PublicApiGeneratedModelWireFormatTest {
  private val objectMapper = ObjectMapper().findAndRegisterModules()
  private val yamlMapper = YAMLMapper().findAndRegisterModules()

  @Test
  fun `all config yaml object fields have generated public API wire-name annotations`() {
    val expectedFields = configYamlObjectFields()
    val missingFields =
      expectedFields
        .filterNot { (schemaName, fieldName) -> generatedModelSource(schemaName).hasJsonPropertyField(fieldName) }

    assertTrue(expectedFields.size > 1_000, "Expected to validate the full config.yaml model field surface")
    assertTrue(
      missingFields.isEmpty(),
      "Generated public API models are missing @JsonProperty wire-name annotations for config.yaml fields:\n" +
        missingFields.joinToString("\n") { (schemaName, fieldName) -> "$schemaName.$fieldName" },
    )
  }

  @Test
  fun `application token request uses OpenAPI wire field names`() {
    val request =
      objectMapper.readValue<ApplicationTokenRequestWithGrant>(
        """
        {
          "client_id": "client-id",
          "client_secret": "client-secret",
          "grant-type": "client_credentials"
        }
        """.trimIndent(),
      )

    assertEquals("client-id", request.clientId)
    assertEquals("client-secret", request.clientSecret)
    assertEquals(ApplicationTokenRequestWithGrant.GrantType.CLIENT_CREDENTIALS, request.grantType)

    val json = objectMapper.writeValueAsString(request)

    assertContainsField(json, "client_id")
    assertContainsField(json, "client_secret")
    assertContainsField(json, "grant-type")
    assertDoesNotContainField(json, "clientId")
    assertDoesNotContainField(json, "clientSecret")
    assertDoesNotContainField(json, "grantType")
  }

  @Test
  fun `access token response uses OpenAPI wire field names`() {
    val response =
      PublicAccessTokenResponse(
        accessToken = "access-token",
        tokenType = PublicAccessTokenResponse.TokenType.BEARER,
        expiresIn = 3600,
      )

    val json = objectMapper.writeValueAsString(response)

    assertContainsField(json, "access_token")
    assertContainsField(json, "token_type")
    assertContainsField(json, "expires_in")
    assertDoesNotContainField(json, "accessToken")
    assertDoesNotContainField(json, "tokenType")
    assertDoesNotContainField(json, "expiresIn")

    val roundTrip = objectMapper.readValue<PublicAccessTokenResponse>(json)

    assertEquals(response, roundTrip)
  }

  @Test
  fun `job status enum accepts queued status`() {
    assertEquals("queued", JobStatusEnum.decode("queued")?.toString())
  }

  private fun assertContainsField(
    json: String,
    fieldName: String,
  ) {
    assertTrue(json.contains("\"$fieldName\":"), "Expected JSON field $fieldName in: $json")
  }

  private fun assertDoesNotContainField(
    json: String,
    fieldName: String,
  ) {
    assertFalse(json.contains("\"$fieldName\":"), "Unexpected JSON field $fieldName in: $json")
  }

  private fun configYamlObjectFields(): List<WireField> {
    val schemas = yamlMapper.readTree(configYamlPath().toFile()).path("components").path("schemas")

    return schemas
      .properties()
      .asSequence()
      .flatMap { (schemaName, schema) ->
        collectObjectPropertyNames(schema).map { fieldName -> WireField(schemaName, fieldName) }
      }.toList()
      .sortedWith(compareBy(WireField::schemaName, WireField::fieldName))
  }

  private fun collectObjectPropertyNames(schema: JsonNode): Set<String> =
    buildSet {
      schema
        .path("properties")
        .fieldNames()
        .asSequence()
        .forEach(::add)
      schema.path("allOf").forEach { addAll(collectObjectPropertyNames(it)) }
    }

  private fun generatedModelSource(schemaName: String): String = Files.readString(generatedModelsPath().resolve("$schemaName.kt"))

  private fun String.hasJsonPropertyField(fieldName: String): Boolean =
    Regex("""@JsonProperty\("${Regex.escape(fieldName)}"\)[\s\S]{0,800}\b(?:val|var)\s+""").containsMatchIn(this)

  private fun configYamlPath(): Path = repositoryRoot().resolve("oss/airbyte-api/server-api/src/main/openapi/config.yaml")

  private fun generatedModelsPath(): Path =
    repositoryRoot().resolve(
      "oss/airbyte-api/public-api/build/generated/public_api/server/src/main/kotlin/io/airbyte/publicApi/server/generated/models",
    )

  private fun repositoryRoot(): Path =
    generateSequence(Path.of("").toAbsolutePath()) { it.parent }
      .first { Files.exists(it.resolve("settings.gradle.kts")) && Files.exists(it.resolve("oss/airbyte-api")) }

  private data class WireField(
    val schemaName: String,
    val fieldName: String,
  )
}
