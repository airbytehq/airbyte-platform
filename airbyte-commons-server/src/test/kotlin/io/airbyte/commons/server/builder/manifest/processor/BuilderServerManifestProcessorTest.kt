/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.manifest.processor

import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadLogsInner
import io.airbyte.commons.json.Jsons
import io.airbyte.connectorbuilderserver.api.client.ConnectorBuilderServerApiClient
import io.airbyte.connectorbuilderserver.api.client.generated.ConnectorBuilderServerApi
import io.airbyte.connectorbuilderserver.api.client.generated.HealthApi
import io.airbyte.connectorbuilderserver.api.client.model.generated.FullResolveManifestRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.HealthCheckRead
import io.airbyte.connectorbuilderserver.api.client.model.generated.HealthCheckReadCapabilities
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpRequest
import io.airbyte.connectorbuilderserver.api.client.model.generated.HttpResponse
import io.airbyte.connectorbuilderserver.api.client.model.generated.ResolveManifest
import io.airbyte.connectorbuilderserver.api.client.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamRead
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadLogsInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilderserver.api.client.model.generated.StreamReadSlicesInnerPagesInner
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class BuilderServerManifestProcessorTest {
  private lateinit var connectorBuilderServerApiClient: ConnectorBuilderServerApiClient
  private lateinit var builderServerApi: ConnectorBuilderServerApi
  private lateinit var healthApi: HealthApi
  private lateinit var processor: BuilderServerManifestProcessor

  private val manifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\"}]}")
  private val configJson = Jsons.deserialize("{\"api_key\": \"test_key\"}")
  private val workspaceId = UUID.randomUUID()
  private val builderProjectId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    connectorBuilderServerApiClient = mockk()
    builderServerApi = mockk()
    healthApi = mockk()

    every { connectorBuilderServerApiClient.builderServerApi } returns builderServerApi
    every { connectorBuilderServerApiClient.healthApi } returns healthApi

    processor = BuilderServerManifestProcessor(connectorBuilderServerApiClient)
  }

  @Test
  fun testResolveManifest() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ResolveManifest(manifest = resolvedManifestJson)

    val expectedRequest =
      ResolveManifestRequestBody(
        manifest = manifestJson,
        projectId = builderProjectId.toString(),
        workspaceId = workspaceId.toString(),
      )

    every { builderServerApi.resolveManifest(expectedRequest) } returns response

    val result = processor.resolveManifest(manifestJson, builderProjectId, workspaceId)

    assertEquals(resolvedManifestJson, result)
    verify { builderServerApi.resolveManifest(expectedRequest) }
  }

  @Test
  fun testResolveManifestWithNullIds() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ResolveManifest(manifest = resolvedManifestJson)

    val expectedRequest =
      ResolveManifestRequestBody(
        manifest = manifestJson,
      )

    every { builderServerApi.resolveManifest(expectedRequest) } returns response

    val result = processor.resolveManifest(manifestJson, null, null)

    assertEquals(resolvedManifestJson, result)
    verify { builderServerApi.resolveManifest(expectedRequest) }
  }

  @Test
  fun testFullResolveManifest() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ResolveManifest(manifest = resolvedManifestJson)
    val streamLimit = 10

    val expectedRequest =
      FullResolveManifestRequestBody(
        config = configJson,
        manifest = manifestJson,
        streamLimit = streamLimit,
        projectId = builderProjectId.toString(),
        workspaceId = workspaceId.toString(),
      )

    every { builderServerApi.fullResolveManifest(expectedRequest) } returns response

    val result = processor.fullResolveManifest(configJson, manifestJson, streamLimit, builderProjectId, workspaceId)

    assertEquals(resolvedManifestJson, result.manifest)
    verify { builderServerApi.fullResolveManifest(expectedRequest) }
  }

  @Test
  fun testFullResolveManifestWithNullParams() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ResolveManifest(manifest = resolvedManifestJson)

    val expectedRequest =
      FullResolveManifestRequestBody(
        config = configJson,
        manifest = manifestJson,
      )

    every { builderServerApi.fullResolveManifest(expectedRequest) } returns response

    val result = processor.fullResolveManifest(configJson, manifestJson, null, null, null)

    assertEquals(resolvedManifestJson, result.manifest)
    verify { builderServerApi.fullResolveManifest(expectedRequest) }
  }

  @Test
  fun testStreamTestRead() {
    val streamName = "test_stream"
    val customCode = "def custom_function(): pass"
    val formGenerated = true
    val recordLimit = 100
    val pageLimit = 5
    val sliceLimit = 3
    val state = listOf<Any>(mapOf("cursor" to "2023-01-01"))

    val expectedRequest =
      StreamReadRequestBody(
        config = configJson,
        manifest = manifestJson,
        stream = streamName,
        customComponentsCode = customCode,
        formGeneratedManifest = formGenerated,
        projectId = builderProjectId.toString(),
        recordLimit = recordLimit,
        pageLimit = pageLimit,
        sliceLimit = sliceLimit,
        state = state,
        workspaceId = workspaceId.toString(),
      )

    val httpRequest = HttpRequest("https://api.example.com/users", HttpRequest.HttpMethod.GET, null, null)
    val httpResponse = HttpResponse(200, "[{\"id\": 1, \"name\": \"John\"}]", null)
    val record = Jsons.deserialize("{\"id\": 1, \"name\": \"John\"}")

    val streamReadResponse =
      StreamRead(
        logs =
          mutableListOf(
            StreamReadLogsInner(
              message = "Test log message",
              level = StreamReadLogsInner.Level.INFO,
              internalMessage = null,
              stacktrace = null,
            ),
          ),
        slices =
          mutableListOf(
            StreamReadSlicesInner(
              pages =
                mutableListOf(
                  StreamReadSlicesInnerPagesInner(
                    records = mutableListOf(record),
                    request = httpRequest,
                    response = httpResponse,
                  ),
                ),
              sliceDescriptor = "{}",
              state = mutableListOf(),
              auxiliaryRequests = mutableListOf(),
            ),
          ),
        testReadLimitReached = false,
        auxiliaryRequests = mutableListOf(),
        inferredSchema = null,
        inferredDatetimeFormats = null,
        latestConfigUpdate = null,
      )

    every { builderServerApi.readStream(expectedRequest) } returns streamReadResponse

    val result =
      processor.streamTestRead(
        configJson,
        manifestJson,
        streamName,
        customCode,
        formGenerated,
        builderProjectId,
        recordLimit,
        pageLimit,
        sliceLimit,
        state,
        workspaceId,
      )

    assertNotNull(result)
    assertEquals(1, result.logs.size)
    assertEquals("Test log message", result.logs[0].message)
    assertEquals(ConnectorBuilderProjectStreamReadLogsInner.LevelEnum.INFO, result.logs[0].level)

    assertEquals(1, result.slices.size)
    assertEquals(1, result.slices[0].pages.size)
    assertEquals(
      1,
      result.slices[0]
        .pages[0]
        .records.size,
    )
    assertEquals(record, result.slices[0].pages[0].records[0])

    assertFalse(result.testReadLimitReached)

    verify { builderServerApi.readStream(expectedRequest) }
  }

  @Test
  fun testGetCapabilities() {
    val healthCheck =
      HealthCheckRead(
        available = true,
        capabilities = HealthCheckReadCapabilities(customCodeExecution = true),
      )

    every { healthApi.getHealthCheck() } returns healthCheck

    val result = processor.getCapabilities()

    assertTrue(result.customCodeExecution)
    verify { healthApi.getHealthCheck() }
  }

  @Test
  fun testGetCapabilitiesWithNullCapabilities() {
    val healthCheck =
      HealthCheckRead(
        available = true,
        capabilities = null,
      )

    every { healthApi.getHealthCheck() } returns healthCheck

    val result = processor.getCapabilities()

    assertFalse(result.customCodeExecution)
    verify { healthApi.getHealthCheck() }
  }

  @Test
  fun testGetCapabilitiesWithNullCustomCodeExecution() {
    val healthCheck =
      HealthCheckRead(
        available = true,
        capabilities = HealthCheckReadCapabilities(customCodeExecution = null),
      )

    every { healthApi.getHealthCheck() } returns healthCheck

    val result = processor.getCapabilities()

    assertFalse(result.customCodeExecution)
    verify { healthApi.getHealthCheck() }
  }

  @Test
  fun testConvertStreamReadWithNullHttpRequestAndResponse() {
    val streamName = "test_stream"
    val record = Jsons.deserialize("{\"id\": 456}")

    val streamReadResponse =
      StreamRead(
        logs = mutableListOf(),
        slices =
          mutableListOf(
            StreamReadSlicesInner(
              pages =
                mutableListOf(
                  StreamReadSlicesInnerPagesInner(
                    records = mutableListOf(record),
                    request = null,
                    response = null,
                  ),
                ),
              sliceDescriptor = null,
              state = mutableListOf(),
              auxiliaryRequests = mutableListOf(),
            ),
          ),
        testReadLimitReached = false,
        auxiliaryRequests = mutableListOf(),
        inferredSchema = null,
        inferredDatetimeFormats = null,
        latestConfigUpdate = null,
      )

    every { builderServerApi.readStream(any()) } returns streamReadResponse

    val result =
      processor.streamTestRead(
        configJson,
        manifestJson,
        streamName,
        null,
        null,
        builderProjectId,
        null,
        null,
        null,
        null,
        workspaceId,
      )

    assertNotNull(result.slices[0].pages[0])
    assertEquals(record, result.slices[0].pages[0].records[0])
    assertEquals(null, result.slices[0].pages[0].request)
    assertEquals(null, result.slices[0].pages[0].response)
  }
}
