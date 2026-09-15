/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.client.generated.SourceExecutionApi
import io.airbyte.api.client.model.generated.SourceExecuteRequest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.TimeUnit

class SourceExecuteApiTest {
  private val objectMapper = ObjectMapper().findAndRegisterModules()

  @Test
  fun `execute sends snake case controls and preserves opaque JSON over HTTP`() {
    val sourceId = UUID.fromString("d19f634e-084b-4d64-95d0-e23598839f14")
    val requestJson =
      """
      {"entity":"contacts","action":"api_search","params":{"filter":[null,{"active":false}],"cursor":null,"id":9223372036854775000},
      "select_fields":["name"],"exclude_fields":[],"skip_truncation":false,"intent":"lookup"}
      """.trimIndent()
    val request = objectMapper.readValue(requestJson, SourceExecuteRequest::class.java)
    assertEquals(JsonNode::class.java, SourceExecuteRequest::class.java.getMethod("getParams").returnType)
    MockWebServer().use { server ->
      server.start()
      val client = SourceExecutionApi(basePath = server.url("/api").toString())
      for (data in listOf("null", "{}", "[]", "false", "0", "\"\"", "[null,{\"id\":9223372036854775000}]")) {
        for (meta in listOf("", """, "meta":{"cursor":null,"extra":[null,{"id":9223372036854775000}]}""")) {
          val responseJson = "{\"data\":$data$meta}"
          server.enqueue(MockResponse().setHeader("Content-Type", "application/json").setBody(responseJson))

          val response: JsonNode = client.executeSource(sourceId.toString(), request)

          assertEquals(objectMapper.readTree(responseJson), response)
          val recorded = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
          assertEquals("POST", recorded.method)
          assertEquals("/api/v1/sources/$sourceId/execute", recorded.path)
          assertEquals(objectMapper.readTree(requestJson), objectMapper.readTree(recorded.body.readUtf8()))
        }
      }
    }
  }

  @Test
  fun `execute client defaults truncation control when omitted`() {
    val request = objectMapper.readValue("""{"entity":"contacts","action":"list"}""", SourceExecuteRequest::class.java)

    assertEquals(true, request.skipTruncation)
  }
}
