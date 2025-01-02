/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.core.JsonParseException
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

class EnterpriseSourceStubsHandlerTest {
  private lateinit var enterpriseSourceHandler: EnterpriseSourceStubsHandler
  private lateinit var mockWebServer: MockWebServer

  @BeforeEach
  fun setUp() {
    mockWebServer = MockWebServer()
    mockWebServer.start()
    val baseUrl = mockWebServer.url("/").toString()

    enterpriseSourceHandler = EnterpriseSourceStubsHandler(baseUrl, 5000)
  }

  @AfterEach
  fun tearDown() {
    mockWebServer.shutdown()
  }

  @Test
  fun testListEnterpriseSourceStubs_Success() {
    val mockJsonResponse = "[{\"name\":\"Test Source\",\"id\":\"test-id\"}]"
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody(mockJsonResponse)
        .addHeader("Content-Type", "application/json"),
    )

    val result = enterpriseSourceHandler.listEnterpriseSourceStubs()

    assertNotNull(result)
    assertEquals(1, result.enterpriseSourceStubs.size)
    assertEquals("Test Source", result.enterpriseSourceStubs[0].name)
    assertEquals("test-id", result.enterpriseSourceStubs[0].id)
  }

  @Test
  fun testListEnterpriseSourceStubs_HttpError() {
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(400)
        .setBody("Bad Request"),
    )

    assertThrows<IOException> {
      enterpriseSourceHandler.listEnterpriseSourceStubs()
    }
  }

  @Test
  fun testListEnterpriseSourceStubs_InvalidJsonResponse() {
    val invalidJsonResponse = "Invalid JSON"
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody(invalidJsonResponse)
        .addHeader("Content-Type", "application/json"),
    )

    val exception =
      assertThrows<IOException> {
        enterpriseSourceHandler.listEnterpriseSourceStubs()
      }

    // Check that the exception message matches what we expect
    assertEquals("HTTP error fetching enterprise sources", exception.message)

    // If you want to verify that the cause is indeed a JsonParseException, you can do:
    assertTrue(exception.cause is JsonParseException)
  }

  @Test
  fun testListEnterpriseSourceStubs_EmptyResponse() {
    mockWebServer.enqueue(
      MockResponse()
        .setResponseCode(200)
        .setBody("")
        .addHeader("Content-Type", "application/json"),
    )

    assertThrows<IOException> {
      enterpriseSourceHandler.listEnterpriseSourceStubs()
    }
  }
}
