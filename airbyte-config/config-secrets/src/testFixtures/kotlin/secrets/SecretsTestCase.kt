/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.lang.Exceptions
import io.airbyte.commons.resources.Resources
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.io.IOException
import java.util.Arrays
import java.util.UUID
import java.util.function.Consumer

/**
 * Provides an easy way of accessing a set of resource files in a specific directory when testing
 * secrets-related helpers.
 */
interface SecretsTestCase {
  val name: String
  val firstSecretMap: Map<AirbyteManagedSecretCoordinate, String>
  val secondSecretMap: Map<AirbyteManagedSecretCoordinate, String>
  val persistenceUpdater: Consumer<SecretPersistence>
  val spec: ConnectorSpecification
    get() =
      Exceptions.toRuntime<ConnectorSpecification> {
        ConnectorSpecification().withConnectionSpecification(
          getNodeResource(name, "spec.json"),
        )
      }
  val fullConfig: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(name, "full_config.json")
      }
    }
  val partialConfig: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(name, "partial_config.json")
      }
    }
  val sortedPartialConfig: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(name, "partial_config.json")
      }
    }
  val updateConfig: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(name, "update_config.json")
      }
    }
  val updatedPartialConfig: JsonNode
    get() {
      return Exceptions.toRuntime<JsonNode> {
        getNodeResource(name, "updated_partial_config.json")
      }
    }

  @Throws(IOException::class)
  fun getNodeResource(
    testCase: String,
    fileName: String,
  ): JsonNode = Jsons.deserialize(Resources.read("$testCase/$fileName"))

  @get:Throws(IOException::class)
  val expectedSecretsPaths: List<String>
    get() {
      return Arrays
        .stream(
          Resources
            .read("$name/expectedPaths")
            .trim { it <= ' ' }
            .split(";".toRegex())
            .dropLastWhile { it.isEmpty() }
            .toTypedArray(),
        ).sorted()
        .toList()
    }

  companion object {
    private const val PREFIX: String = "airbyte_workspace_"
    private const val SECRET: String = "_secret_"

    val WORKSPACE_ID: UUID = UUID.fromString("e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2")

    // use a fixed sequence of UUIDs so it's easier to have static files for the test cases
    val UUIDS =
      listOf(
        UUID.fromString("9eba44d8-51e7-48f1-bde2-619af0e42c22"),
        UUID.fromString("2c2ef2b3-259a-4e73-96d1-f56dacee2e5e"),
        UUID.fromString("1206db5b-b968-4df1-9a76-f3fcdae7e307"),
        UUID.fromString("c03ef566-79a7-4e77-b6f3-d23d2528f25a"),
        UUID.fromString("35f08b15-bfd9-44fe-a8c7-5aa9e156c0f5"),
        UUID.fromString("159c0b6f-f9ae-48b4-b7f3-bcac4ba15743"),
        UUID.fromString("71af9b74-4e61-4cff-830e-3bf1ec18fbc0"),
        UUID.fromString("067a62fc-d007-44dd-a8f6-0fd10823713d"),
        UUID.fromString("c4967ac9-0856-4733-a21e-1d51ca8f254d"),
      )

    fun buildBaseCoordinate(
      prefix: String = PREFIX,
      workspaceId: UUID = WORKSPACE_ID,
      secret: String = SECRET,
      uuidIndex: Int,
    ): String = prefix + workspaceId + secret + UUIDS[uuidIndex]
  }
}
