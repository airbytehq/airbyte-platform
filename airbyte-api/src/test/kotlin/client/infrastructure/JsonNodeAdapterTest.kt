/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.fasterxml.jackson.databind.JsonNode
import com.squareup.moshi.adapter
import io.airbyte.api.client2.model.generated.AirbyteCatalog
import io.airbyte.api.client2.model.generated.AirbyteStream
import io.airbyte.api.client2.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client2.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client2.model.generated.ConnectionRead
import io.airbyte.api.client2.model.generated.ConnectionReadList
import io.airbyte.api.client2.model.generated.ConnectionStatus
import io.airbyte.api.client2.model.generated.DestinationSyncMode
import io.airbyte.api.client2.model.generated.SyncMode
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer
import java.util.UUID

internal class JsonNodeAdapterTest {
  private lateinit var data: Map<String, Any>
  private lateinit var jsonNode: JsonNode
  private lateinit var jsonString: String
  private lateinit var adapter: JsonNodeAdapter

  @BeforeEach
  internal fun setup() {
    data = mapOf("key1" to "value1")
    jsonNode = Jsons.jsonNode(data)
    jsonString = Jsons.serialize(data)
    adapter = JsonNodeAdapter()
  }

  @Test
  internal fun toJson() {
    assertEquals(data, adapter.toJson(jsonNode))
  }

  @Test
  internal fun fromJson() {
    assertEquals(jsonNode, adapter.fromJson(data))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testDeserialization() {
    assertEquals(jsonNode, Serializer.moshi.adapter<JsonNode>().fromJson(jsonString))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerialization() {
    assertEquals(jsonString, Serializer.moshi.adapter<JsonNode>().toJson(jsonNode))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeGeneratedModelObject() {
    val connectionRead =
      ConnectionRead(
        connectionId = UUID.randomUUID(),
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog =
          AirbyteCatalog(
            listOf(
              AirbyteStreamAndConfiguration(
                stream =
                  AirbyteStream(
                    jsonSchema = jsonNode,
                    name = "stream",
                  ),
                config =
                  AirbyteStreamConfiguration(
                    syncMode = SyncMode.INCREMENTAL,
                    destinationSyncMode = DestinationSyncMode.APPEND,
                  ),
              ),
            ),
          ),
        status = ConnectionStatus.ACTIVE,
        name = "name",
        breakingChange = false,
      )
    val connectionReadList = ConnectionReadList(listOf(connectionRead))
    val adapter = Serializer.moshi.adapter<ConnectionReadList>()
    val json = adapter.toJson(connectionReadList)
    assertEquals(connectionReadList, adapter.fromJson(json))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testSerdeGeneratedArrayOfJsonNodes() {
    val resp =
      TestResponse(
        sampleArray = listOf(Jsons.jsonNode(mapOf("foo" to "bar"))),
        otherField = 5,
      )
    val adapter = Serializer.moshi.adapter<TestResponse>()
    val json = adapter.toJson(resp)
    println(json)
    assertEquals(resp, adapter.fromJson(json))
  }

  internal class TestResponse(val sampleArray: List<JsonNode>, val otherField: Int) {
    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (javaClass != other?.javaClass) return false

      other as TestResponse

      if (sampleArray != other.sampleArray) return false
      if (otherField != other.otherField) return false

      return true
    }

    override fun hashCode(): Int {
      var result = sampleArray.hashCode()
      result = 31 * result + otherField
      return result
    }
  }
}
