/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import com.squareup.moshi.adapter
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionReadList
import io.airbyte.api.client.model.generated.ConnectionState
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.api.client.model.generated.ConnectionStateType
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SourceCreate
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.StreamDescriptor
import io.airbyte.api.client.model.generated.StreamState
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.openapitools.client.infrastructure.Serializer
import java.util.UUID

internal class JsonNodeAdapterTest {
  companion object {
    private val emptyMap = emptyMap<String, Any>()
    private val emptyNode = Jsons.jsonNode(emptyMap)
  }

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
    assertEquals(emptyMap, adapter.toJson(null))
    assertEquals(emptyMap, adapter.toJson(NullNode.getInstance()))
  }

  @Test
  internal fun fromJson() {
    assertEquals(jsonNode, adapter.fromJson(data))
    assertEquals(emptyNode, adapter.fromJson(null))
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
    assertEquals(resp, adapter.fromJson(json))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testHandlingOfNumbersInJsonNodes() {
    val create =
      SourceCreate(
        name = "name",
        workspaceId = UUID.randomUUID(),
        sourceDefinitionId = UUID.randomUUID(),
        secretId = "secret",
        connectionConfiguration =
          Jsons.jsonNode(
            mapOf(
              "host" to "127.0.0.1",
              "port" to 12345L,
              "database" to "test",
              "ssl" to false,
              "username" to "user",
              "password" to "password",
              "sampling_ratio" to 123.45,
            ),
          ),
      )
    val adapter = Serializer.moshi.adapter<SourceCreate>()
    val json = adapter.toJson(create)
    assertEquals(create, adapter.fromJson(json))
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  @Suppress("UNCHECKED_CAST")
  internal fun testIntegerNormalizationInConnectorConfiguration() {
    val json = MoreResources.readResource("json/responses/jobs_get_check_input_response.json")
    val adapter = Serializer.moshi.adapter<Any>()
    val result = transformNumbersToInts(adapter.fromJson(json) as Map<String, Any>)
    assertEquals(62371L, ((result["sourceCheckConnectionInput"] as Map<String, Any>)["connectionConfiguration"] as Map<String, Any>)["port"])
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  @Suppress("UNCHECKED_CAST")
  internal fun testHandlingOfNumbersInAListJsonNodes() {
    val json = MoreResources.readResource("json/responses/source_read_response.json")
    val adapter = Serializer.moshi.adapter<SourceRead>()
    var result = adapter.fromJson(json)
    assertEquals(1234567890, result?.connectionConfiguration?.get("account_ids")?.first()?.asInt())
  }

  @Test
  @OptIn(ExperimentalStdlibApi::class)
  internal fun testHandlingNullJsonNode() {
    // null cannot be cast to non-null type kotlin.collections.Map<kotlin.String, kotlin.Any> at $.connectionState.streamState[0].streamState
    val connectionState =
      ConnectionState(
        connectionId = UUID.randomUUID(),
        streamState =
          listOf(
            StreamState(
              streamDescriptor = StreamDescriptor(name = "name", namespace = "namespace"),
              streamState = null,
            ),
          ),
        state = null,
        stateType = ConnectionStateType.STREAM,
        globalState = null,
      )
    val update =
      ConnectionStateCreateOrUpdate(
        connectionId = connectionState.connectionId,
        connectionState = connectionState,
      )

    val adapter = Serializer.moshi.adapter<ConnectionStateCreateOrUpdate>()
    val json = adapter.toJson(update)
    val result = adapter.fromJson(json)
    assertEquals(update, result)
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
