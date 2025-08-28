/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.manifest.processor

import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadLogsInner
import io.airbyte.commons.json.Jsons
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.airbyte.manifestserver.api.client.generated.CapabilitiesApi
import io.airbyte.manifestserver.api.client.generated.ManifestApi
import io.airbyte.manifestserver.api.client.model.generated.AuxiliaryRequest
import io.airbyte.manifestserver.api.client.model.generated.CapabilitiesResponse
import io.airbyte.manifestserver.api.client.model.generated.FullResolveRequest
import io.airbyte.manifestserver.api.client.model.generated.HttpRequest
import io.airbyte.manifestserver.api.client.model.generated.HttpResponse
import io.airbyte.manifestserver.api.client.model.generated.LogMessage
import io.airbyte.manifestserver.api.client.model.generated.ManifestResponse
import io.airbyte.manifestserver.api.client.model.generated.ResolveRequest
import io.airbyte.manifestserver.api.client.model.generated.StreamReadPages
import io.airbyte.manifestserver.api.client.model.generated.StreamReadResponse
import io.airbyte.manifestserver.api.client.model.generated.StreamReadSlices
import io.airbyte.manifestserver.api.client.model.generated.StreamTestReadRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

internal class ManifestServerManifestProcessorTest {
  private lateinit var manifestServerApiClient: ManifestServerApiClient
  private lateinit var manifestApi: ManifestApi
  private lateinit var capabilitiesApi: CapabilitiesApi
  private lateinit var processor: ManifestServerManifestProcessor

  private val manifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\"}]}")
  private val configJson = Jsons.deserialize("{\"api_key\": \"test_key\"}")
  private val workspaceId = UUID.randomUUID()
  private val builderProjectId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    manifestServerApiClient = mockk()
    manifestApi = mockk()
    capabilitiesApi = mockk()

    every { manifestServerApiClient.manifestApi } returns manifestApi
    every { manifestServerApiClient.capabilitiesApi } returns capabilitiesApi

    processor = ManifestServerManifestProcessor(manifestServerApiClient)
  }

  @Test
  fun testResolveManifest() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ManifestResponse(manifest = resolvedManifestJson)

    val expectedRequest = ResolveRequest(manifest = manifestJson)

    every { manifestApi.resolve(expectedRequest) } returns response

    val result = processor.resolveManifest(manifestJson, builderProjectId, workspaceId)

    assertEquals(resolvedManifestJson, result)
    verify { manifestApi.resolve(expectedRequest) }
  }

  @Test
  fun testResolveManifestWithNullIds() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ManifestResponse(manifest = resolvedManifestJson)

    val expectedRequest = ResolveRequest(manifest = manifestJson)

    every { manifestApi.resolve(expectedRequest) } returns response

    val result = processor.resolveManifest(manifestJson, null, null)

    assertEquals(resolvedManifestJson, result)
    verify { manifestApi.resolve(expectedRequest) }
  }

  @Test
  fun testFullResolveManifest() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ManifestResponse(manifest = resolvedManifestJson)
    val streamLimit = 10

    val expectedRequest =
      FullResolveRequest(
        config = configJson,
        manifest = manifestJson,
        streamLimit = streamLimit,
      )

    every { manifestApi.fullResolve(expectedRequest) } returns response

    val result = processor.fullResolveManifest(configJson, manifestJson, streamLimit, builderProjectId, workspaceId)

    assertEquals(resolvedManifestJson, result.manifest)
    verify { manifestApi.fullResolve(expectedRequest) }
  }

  @Test
  fun testFullResolveManifestWithNullParams() {
    val resolvedManifestJson = Jsons.deserialize("{\"streams\": [{\"name\": \"test\", \"url_base\": \"https://api.example.com\"}]}")
    val response = ManifestResponse(manifest = resolvedManifestJson)

    val expectedRequest =
      FullResolveRequest(
        config = configJson,
        manifest = manifestJson,
        streamLimit = null,
      )

    every { manifestApi.fullResolve(expectedRequest) } returns response

    val result = processor.fullResolveManifest(configJson, manifestJson, null, null, null)

    assertEquals(resolvedManifestJson, result.manifest)
    verify { manifestApi.fullResolve(expectedRequest) }
  }

  @Test
  fun testStreamTestRead() {
    val streamName = "test_stream"
    val customCode = "def custom_function(): pass"
    val recordLimit = 100
    val pageLimit = 5
    val sliceLimit = 3
    val state = listOf<Any>(mapOf("cursor" to "2023-01-01"))

    val expectedRequest =
      StreamTestReadRequest(
        config = configJson,
        manifest = manifestJson,
        streamName = streamName,
        state = state,
        customComponentsCode = customCode,
        recordLimit = recordLimit,
        pageLimit = pageLimit,
        sliceLimit = sliceLimit,
      )

    val httpRequest = HttpRequest("https://api.example.com/users", null, "GET", null)
    val httpResponse = HttpResponse(200, "[{\"id\": 1, \"name\": \"John\"}]", null)
    val record = Jsons.deserialize("{\"id\": 1, \"name\": \"John\"}")

    val streamReadResponse =
      StreamReadResponse(
        logs =
          listOf(
            LogMessage(
              message = "Test log message",
              level = "info",
              internalMessage = null,
              stacktrace = null,
            ),
          ),
        slices =
          listOf(
            StreamReadSlices(
              pages =
                listOf(
                  StreamReadPages(
                    records = listOf(record),
                    request = httpRequest,
                    response = httpResponse,
                  ),
                ),
              sliceDescriptor = "{\"partition\": \"2023\"}",
              state = listOf(),
              auxiliaryRequests = listOf(),
            ),
          ),
        testReadLimitReached = false,
        auxiliaryRequests = listOf(),
        inferredSchema = null,
        inferredDatetimeFormats = null,
        latestConfigUpdate = null,
      )

    every { manifestApi.testRead(expectedRequest) } returns streamReadResponse

    val result =
      processor.streamTestRead(
        configJson,
        manifestJson,
        streamName,
        customCode,
        null,
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

    verify { manifestApi.testRead(expectedRequest) }
  }

  @Test
  fun testStreamTestReadMinimalParams() {
    val streamName = "test_stream"

    val expectedRequest =
      StreamTestReadRequest(
        config = configJson,
        manifest = manifestJson,
        streamName = streamName,
        state = null,
        customComponentsCode = null,
        recordLimit = null,
        pageLimit = null,
        sliceLimit = null,
      )

    val streamReadResponse =
      StreamReadResponse(
        logs = listOf(),
        slices = listOf(),
        testReadLimitReached = false,
        auxiliaryRequests = listOf(),
        inferredSchema = null,
        inferredDatetimeFormats = null,
        latestConfigUpdate = null,
      )

    every { manifestApi.testRead(expectedRequest) } returns streamReadResponse

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

    assertNotNull(result)
    assertTrue(result.logs.isEmpty())
    assertTrue(result.slices.isEmpty())
    assertFalse(result.testReadLimitReached)

    verify { manifestApi.testRead(expectedRequest) }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGetCapabilitiesWithNullValue(customCodeExecution: Boolean) {
    every { capabilitiesApi.getCapabilities() } returns CapabilitiesResponse(customCodeExecution = customCodeExecution)

    val result = processor.getCapabilities()

    assertEquals(customCodeExecution, result.customCodeExecution)
    verify { capabilitiesApi.getCapabilities() }
  }

  @Test
  fun testStreamTestReadWithAuxiliaryRequests() {
    val streamName = "test_stream"

    val auxiliaryRequest =
      AuxiliaryRequest(
        title = "OAuth Token Request",
        type = "http",
        description = "Request to obtain OAuth token",
        request = HttpRequest("https://api.example.com/oauth/token", null, "POST", "grant_type=client_credentials"),
        response = HttpResponse(200, "{\"access_token\": \"abc123\"}", null),
      )

    val sliceAuxRequest =
      AuxiliaryRequest(
        title = "Data Request",
        type = "http",
        description = "Request for slice data",
        request = HttpRequest("https://api.example.com/data", null, "GET", null),
        response = HttpResponse(200, "{\"data\": []}", null),
      )

    val streamReadResponse =
      StreamReadResponse(
        logs = listOf(),
        slices =
          listOf(
            StreamReadSlices(
              pages = listOf(),
              sliceDescriptor = null,
              state = null,
              auxiliaryRequests = listOf(sliceAuxRequest),
            ),
          ),
        testReadLimitReached = false,
        auxiliaryRequests = listOf(auxiliaryRequest),
        inferredSchema = null,
        inferredDatetimeFormats = null,
        latestConfigUpdate = null,
      )

    every { manifestApi.testRead(any()) } returns streamReadResponse

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

    assertEquals(1, result.auxiliaryRequests?.size)
    assertEquals("OAuth Token Request", result.auxiliaryRequests?.get(0)?.title)
    assertEquals("Request to obtain OAuth token", result.auxiliaryRequests?.get(0)?.description)

    assertEquals(1, result.slices.size)
    assertEquals(1, result.slices[0].auxiliaryRequests?.size)
    assertEquals(
      "Data Request",
      result.slices[0]
        .auxiliaryRequests
        ?.get(0)
        ?.title,
    )
  }

  @Test
  fun testStreamTestReadWithInferredData() {
    val streamName = "test_stream"
    val inferredSchema = mapOf<String, Any>("type" to "object", "properties" to mapOf("id" to mapOf("type" to "integer")))
    val inferredDatetimeFormats = mapOf("created_at" to "%Y-%m-%d")

    val streamReadResponse =
      StreamReadResponse(
        logs = listOf(),
        slices = listOf(),
        testReadLimitReached = true,
        auxiliaryRequests = listOf(),
        inferredSchema = inferredSchema,
        inferredDatetimeFormats = inferredDatetimeFormats,
        latestConfigUpdate = null,
      )

    every { manifestApi.testRead(any()) } returns streamReadResponse

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

    assertTrue(result.testReadLimitReached)
    assertEquals(inferredSchema, result.inferredSchema)
    assertEquals(inferredDatetimeFormats, result.inferredDatetimeFormats)
  }

  @Test
  fun testConvertStreamReadResponseWithNullHttpRequestAndResponse() {
    val streamName = "test_stream"
    val record = Jsons.deserialize("{\"id\": 456}")

    val streamReadResponse =
      StreamReadResponse(
        logs = listOf(),
        slices =
          listOf(
            StreamReadSlices(
              pages =
                listOf(
                  StreamReadPages(
                    records = listOf(record),
                    request = null,
                    response = null,
                  ),
                ),
              sliceDescriptor = null,
              state = null,
              auxiliaryRequests = null,
            ),
          ),
        testReadLimitReached = false,
        auxiliaryRequests = listOf(),
        inferredSchema = null,
        inferredDatetimeFormats = null,
        latestConfigUpdate = null,
      )

    every { manifestApi.testRead(any()) } returns streamReadResponse

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
