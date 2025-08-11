/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.api.model.generated.StreamReadLogsInner
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilder.api.model.generated.StreamReadSlicesInner
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequester
import org.assertj.core.util.Lists
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException

internal class StreamHandlerTest {
  val objectMapper: ObjectMapper = ObjectMapper()
  var logs: List<StreamReadLogsInner> =
    objectMapper.readValue<List<StreamReadLogsInner>>(
      jsonLogs,
      object : TypeReference<List<StreamReadLogsInner>>() {},
    )
  var slices: List<StreamReadSlicesInner> =
    objectMapper.readValue<List<StreamReadSlicesInner>>(
      jsonSlice,
      object : TypeReference<List<StreamReadSlicesInner>>() {},
    )

  private val streamRead: StreamRead = StreamRead().logs(logs).slices(slices)
  private lateinit var requester: AirbyteCdkRequester
  private lateinit var handler: StreamHandler

  @BeforeEach
  fun setUp() {
    requester = Mockito.mock(AirbyteCdkRequester::class.java)
    handler = StreamHandler(requester)
  }

  @Test
  @Throws(Exception::class)
  fun whenReadStreamThenReturnRequesterResponse() {
    Mockito
      .`when`(
        requester.readStream(
          aManifest!!,
          null,
          aConfig!!,
          aState,
          A_STREAM,
          A_LIMIT,
          A_LIMIT,
          A_LIMIT,
        ),
      ).thenReturn(streamRead)
    val response =
      handler.readStream(
        StreamReadRequestBody()
          .manifest(aManifest)
          .config(aConfig)
          .state(aState)
          .stream(A_STREAM)
          .recordLimit(
            A_LIMIT,
          ).pageLimit(A_LIMIT)
          .sliceLimit(A_LIMIT),
      )
    Assertions.assertEquals(streamRead, response)
  }

  @Test
  @Throws(Exception::class)
  fun givenIOExceptionWhenReadStreamThenRaiseConnectorBuilderException() {
    Mockito
      .`when`(
        requester.readStream(
          aManifest!!,
          null,
          aConfig!!,
          aState,
          A_STREAM,
          A_LIMIT,
          A_LIMIT,
          A_LIMIT,
        ),
      ).thenThrow(
        IOException::class.java,
      )
    Assertions.assertThrows(
      ConnectorBuilderException::class.java,
    ) {
      handler.readStream(
        StreamReadRequestBody()
          .manifest(aManifest)
          .config(aConfig)
          .stream(A_STREAM)
          .state(aState)
          .recordLimit(A_LIMIT)
          .pageLimit(A_LIMIT)
          .sliceLimit(A_LIMIT),
      )
    }
  }

  @Test
  @Throws(Exception::class)
  fun givenAirbyteCdkInvalidInputExceptionWhenReadStreamThenRaiseConnectorBuilderException() {
    Mockito
      .`when`(
        requester.readStream(
          aManifest!!,
          null,
          aConfig!!,
          aState,
          A_STREAM,
          A_LIMIT,
          A_LIMIT,
          A_LIMIT,
        ),
      ).thenThrow(AirbyteCdkInvalidInputException::class.java)
    Assertions.assertThrows(
      AirbyteCdkInvalidInputException::class.java,
    ) {
      handler.readStream(
        StreamReadRequestBody()
          .manifest(aManifest)
          .config(aConfig)
          .stream(A_STREAM)
          .state(aState)
          .recordLimit(A_LIMIT)
          .pageLimit(A_LIMIT)
          .sliceLimit(A_LIMIT),
      )
    }
  }

  companion object {
    private val jsonLogs =
      """
      [
        {
          "message":"slice:{}",
          "level":"INFO"
        }
      ]
      """.trimIndent()
    private val jsonSlice =
      """
      [
        {
          "pages": [
            {
              "records": []
            }
          ]
        }
      ]
      
      """.trimIndent()
    private var aConfig: JsonNode? = null
    private var aManifest: JsonNode? = null
    private var aState: List<JsonNode>? = null
    private const val A_STREAM = "test"
    private const val A_LIMIT = 1

    init {
      try {
        aConfig = ObjectMapper().readTree("{\"config\": 1}")
        aManifest = ObjectMapper().readTree("{\"manifest\": 1}")
        aState = Lists.newArrayList(ObjectMapper().readTree("{}"))
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }
  }
}
