/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.requester

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.airbyte.connectorbuilder.api.model.generated.AuxiliaryRequest
import io.airbyte.connectorbuilder.api.model.generated.StreamReadLogsInner
import io.airbyte.connectorbuilder.api.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilder.commandrunner.SynchronousCdkCommandRunner
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.KArgumentCaptor
import java.io.IOException

internal class AirbyteCdkRequesterImplTest {
  lateinit var commandRunner: SynchronousCdkCommandRunner
  lateinit var requester: AirbyteCdkRequesterImpl

  @BeforeEach
  fun setUp() {
    commandRunner = Mockito.mock(SynchronousCdkCommandRunner::class.java)
    requester = AirbyteCdkRequesterImpl(commandRunner)
  }

  @Throws(Exception::class)
  fun testReadStreamSuccess(
    recordLimit: Int?,
    pageLimit: Int?,
    sliceLimit: Int?,
  ): KArgumentCaptor<String> {
    val mapper = ObjectMapper()
    mapper.registerModule(JavaTimeModule())

    val response =
      mapper.readTree(
        (
          "{\"test_read_limit_reached\": true, \"logs\":[{\"message\":\"log message1\", \"level\":\"INFO\"}, {\"message\":\"log message2\", " +
            "\"level\":\"INFO\"}], \"slices\": [{\"pages\": [{\"records\": [{\"record\": 1}]}], \"slice_descriptor\": {\"startDatetime\": " +
            "\"2023-11-01T00:00:00+00:00\", \"listItem\": \"item\"}, \"state\": [{\"airbyte\": \"state\"}]}, {\"pages\": []}]," +
            "\"inferred_schema\": {\"schema\": 1}, \"latest_config_update\": { \"config_key\": \"config_value\"}," +
            "\"auxiliary_requests\": [{\"title\": \"Refresh token\",\"description\": \"Obtains access token\",\"request\": {\"url\": " +
            "\"https://a-url.com/oauth2/v1/tokens/bearer\",\"headers\": {\"Content-Type\": " +
            "\"application/x-www-form-urlencoded\"},\"http_method\": \"POST\",\"body\": \"a_request_body\"},\"response\": {\"status\": 200," +
            "\"body\": \"a_response_body\",\"headers\": {\"Date\": \"Tue, 11 Jul 2023 16:28:10 GMT\"}}}]}"
        ),
      )
    val configCaptor = org.mockito.kotlin.argumentCaptor<String>()
    Mockito
      .`when`(
        commandRunner.runCommand(
          org.mockito.kotlin.eq(READ_STREAM_COMMAND),
          configCaptor.capture(),
          org.mockito.kotlin.any(),
          org.mockito.kotlin.any(),
        ),
      ).thenReturn(AirbyteRecordMessage().withData(response))

    val streamRead =
      requester.readStream(
        aManifest!!,
        null,
        aConfig!!,
        aState,
        A_STREAM,
        recordLimit,
        pageLimit,
        sliceLimit,
      )

    val testReadLimitReached = mapper.convertValue(response["test_read_limit_reached"], object : TypeReference<Boolean?>() {})
    Assertions.assertEquals(testReadLimitReached, streamRead.testReadLimitReached)

    Assertions.assertEquals(2, streamRead.slices.size)
    val slices: List<StreamReadSlicesInner> =
      mapper.convertValue<List<StreamReadSlicesInner>>(
        response["slices"],
        object : TypeReference<List<StreamReadSlicesInner>>() {},
      )
    Assertions.assertEquals(slices, streamRead.slices)

    Assertions.assertEquals(2, streamRead.logs.size)
    val logs: List<StreamReadLogsInner> =
      mapper.convertValue<List<StreamReadLogsInner>>(
        response["logs"],
        object : TypeReference<List<StreamReadLogsInner>>() {},
      )
    Assertions.assertEquals(logs, streamRead.logs)

    val auxiliaryRequests: List<AuxiliaryRequest> =
      mapper.convertValue<List<AuxiliaryRequest>>(
        response["auxiliary_requests"],
        object : TypeReference<List<AuxiliaryRequest>>() {},
      )
    Assertions.assertEquals(auxiliaryRequests, streamRead.auxiliaryRequests)

    return configCaptor
  }

  @Test
  @Throws(Exception::class)
  fun whenReadStreamWithLimitsThenReturnAdaptedCommandRunnerResponse() {
    // If all test read limits are present, all are passed along in command config
    val configCaptor = testReadStreamSuccess(A_LIMIT, A_LIMIT, A_LIMIT)
    assertRunCommandArgs(configCaptor, READ_STREAM_COMMAND, A_LIMIT, A_LIMIT, A_LIMIT)

    // Test read limits which are not present will not be in the adapted command config
    val noRecordLimitConfigCaptor = testReadStreamSuccess(null, A_LIMIT, A_LIMIT)
    assertRunCommandArgs(noRecordLimitConfigCaptor, READ_STREAM_COMMAND, null, A_LIMIT, A_LIMIT)

    val noPageLimitConfigCaptor = testReadStreamSuccess(A_LIMIT, null, A_LIMIT)
    assertRunCommandArgs(noPageLimitConfigCaptor, READ_STREAM_COMMAND, A_LIMIT, null, A_LIMIT)

    val noSliceLimitConfigCaptor = testReadStreamSuccess(A_LIMIT, A_LIMIT, null)
    assertRunCommandArgs(noSliceLimitConfigCaptor, READ_STREAM_COMMAND, A_LIMIT, A_LIMIT, null)

    // If any of the test read limits get special handling, it may be worth validating a
    // more exhaustive set of permutations, but for now this should be plenty
    val onlyPageLimitConfigCaptor = testReadStreamSuccess(null, A_LIMIT, null)
    assertRunCommandArgs(onlyPageLimitConfigCaptor, READ_STREAM_COMMAND, null, A_LIMIT, null)
  }

  @Test
  @Throws(Exception::class)
  fun whenReadStreamWithExcessiveLimitsThenThrowException() {
    Assertions.assertThrows(
      AirbyteCdkInvalidInputException::class.java,
    ) {
      testReadStreamSuccess(
        AirbyteCdkRequesterImpl.MAX_RECORD_LIMIT + 1,
        A_LIMIT,
        A_LIMIT,
      )
    }
    Assertions.assertThrows(
      AirbyteCdkInvalidInputException::class.java,
    ) {
      testReadStreamSuccess(
        A_LIMIT,
        AirbyteCdkRequesterImpl.MAX_PAGE_LIMIT + 1,
        A_LIMIT,
      )
    }
    Assertions.assertThrows(
      AirbyteCdkInvalidInputException::class.java,
    ) {
      testReadStreamSuccess(
        A_LIMIT,
        A_LIMIT,
        AirbyteCdkRequesterImpl.MAX_SLICE_LIMIT + 1,
      )
    }
  }

  @Test
  @Throws(Exception::class)
  fun whenReadStreamWithoutLimitThenReturnAdaptedCommandRunnerResponse() {
    val configCaptor = testReadStreamSuccess(null, null, null)
    assertRunCommandArgs(configCaptor, READ_STREAM_COMMAND, null, null, null)
  }

  @Test
  @Throws(Exception::class)
  fun givenStreamIsNullWhenReadStreamThenThrowException() {
    Mockito
      .`when`(
        commandRunner.runCommand(
          org.mockito.kotlin.eq(READ_STREAM_COMMAND),
          org.mockito.kotlin.any(),
          org.mockito.kotlin.any(),
          org.mockito.kotlin.any(),
        ),
      ).thenReturn(
        AirbyteRecordMessage().withData(ObjectMapper().readTree("{\"streams\":[{\"name\": \"missing stream\", \"stream\": null}]}")),
      )
    Assertions.assertThrows(AirbyteCdkInvalidInputException::class.java) {
      requester.readStream(
        aManifest!!,
        null,
        aConfig!!,
        aState,
        null,
        A_LIMIT,
        A_LIMIT,
        A_LIMIT,
      )
    }
  }

  @Test
  @Throws(IOException::class)
  fun whenStateIsNotNullAdaptStateConvertsItDirectlyToString() {
    val adaptedState = requester.adaptState(aState)
    Assertions.assertEquals(
      """
      [ {
        "key" : "value"
      } ]
      """.trimIndent(),
      adaptedState,
    )
  }

  @Test
  @Throws(IOException::class)
  fun whenStateIsNullAdaptStateReturnsAnEmptyArray() {
    val adaptedState = requester.adaptState(null)
    Assertions.assertEquals("[ ]", adaptedState)
  }

  @Throws(Exception::class)
  fun assertRunCommandArgs(
    configCaptor: org.mockito.kotlin.KArgumentCaptor<String?>,
    command: String?,
  ) {
    // assert runCommand arguments: We are doing this because the `runCommand` received a JSON as a
    // string and we don't care about the variations in formatting
    // to make this test flaky. Casting the string passed to `runCommand` as a JSON will remove this
    // flakiness
    val config = aConfig!!.deepCopy<JsonNode>()
    (config as ObjectNode).put("__command", command)
    config.set<JsonNode>("__injected_declarative_manifest", aManifest)
    Assertions.assertEquals(config, ObjectMapper().readTree(configCaptor.firstValue))
  }

  @Throws(Exception::class)
  fun assertRunCommandArgs(
    configCaptor: KArgumentCaptor<String>,
    command: String?,
    recordLimit: Int?,
    pageLimit: Int?,
    sliceLimit: Int?,
  ) {
    val config = aConfig!!.deepCopy<JsonNode>()
    (config as ObjectNode).put("__command", command)
    config.set<JsonNode>("__injected_declarative_manifest", aManifest)
    val mapper = ObjectMapper()
    val commandConfig = mapper.createObjectNode()
    if (recordLimit != null) {
      commandConfig.put("max_records", recordLimit)
    }
    if (pageLimit != null) {
      commandConfig.put("max_pages_per_slice", pageLimit)
    }
    if (sliceLimit != null) {
      commandConfig.put("max_slices", sliceLimit)
    }
    config.set<JsonNode>("__test_read_config", commandConfig)
    Assertions.assertEquals(config, ObjectMapper().readTree(configCaptor.firstValue))
  }

  companion object {
    private const val READ_STREAM_COMMAND = "test_read"
    private var aConfig: JsonNode? = null
    private var aManifest: JsonNode? = null
    private var aState: List<JsonNode>? = null
    private const val A_STREAM = "test"
    private const val A_LIMIT = 1

    init {
      try {
        aConfig = ObjectMapper().readTree("{\"config\": 1}")
        aManifest = ObjectMapper().readTree("{\"manifest\": 1}")
        aState = listOf(ObjectMapper().readTree("{\"key\": \"value\"}"))
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }
  }
}
