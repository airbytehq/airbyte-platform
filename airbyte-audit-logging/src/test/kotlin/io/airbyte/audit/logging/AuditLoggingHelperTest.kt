/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.audit.logging.model.Actor
import io.airbyte.commons.server.errors.AuthException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.micronaut.http.HttpHeaders
import io.micronaut.http.server.netty.NettyHttpRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.unmockkAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.nio.charset.Charset
import java.util.UUID

const val TEST_VALUE_UNKNOWN = "unknown"
const val TEST_VALUE_USER_EMAIL = "email@email.com"
const val TEST_VALUE_USER_AGENT = "user-agent"
const val TEST_VALUE_FORWARDED_FOR = "forwarded-for"

class AuditLoggingHelperTest {
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var currentUserService: CurrentUserService
  private lateinit var objectMapper: ObjectMapper
  private lateinit var auditLoggingHelper: AuditLoggingHelper

  @BeforeEach
  fun setUp() {
    permissionHandler = mockk()
    currentUserService = mockk()
    objectMapper = ObjectMapper()
    auditLoggingHelper = AuditLoggingHelper(permissionHandler, currentUserService, objectMapper)
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `buildActor should use empty values if the user cannot be retrieved`() {
    every { currentUserService.getCurrentUser() } throws AuthException("could not get user")

    val request =
      mockRequest(
        mapOf("User-Agent" to TEST_VALUE_USER_AGENT, "X-Forwarded-For" to TEST_VALUE_FORWARDED_FOR),
        "",
      )

    val result = auditLoggingHelper.buildActor(request.headers)
    val expected =
      Actor(
        TEST_VALUE_UNKNOWN,
        null,
        TEST_VALUE_FORWARDED_FOR,
        TEST_VALUE_USER_AGENT,
      )
    assertEquals(expected, result)
  }

  @Test
  fun `buildActor uses values for the current user when available`() {
    val user = AuthenticatedUser()
    val userId = UUID.randomUUID()
    user.userId = userId
    user.email = TEST_VALUE_USER_EMAIL

    every { currentUserService.getCurrentUser() } returns user

    val request =
      mockRequest(
        mapOf("User-Agent" to TEST_VALUE_USER_AGENT, "X-Forwarded-For" to TEST_VALUE_FORWARDED_FOR),
        "",
      )

    val result = auditLoggingHelper.buildActor(request.headers)
    val expected =
      Actor(
        userId.toString(),
        TEST_VALUE_USER_EMAIL,
        TEST_VALUE_FORWARDED_FOR,
        TEST_VALUE_USER_AGENT,
      )
    assertEquals(expected, result)
  }

  @Test
  fun `buildActor returns unknown if the user agent is missing in headers`() {
    val user = AuthenticatedUser()
    val userId = UUID.randomUUID()
    user.userId = userId
    user.email = TEST_VALUE_USER_EMAIL

    every { currentUserService.getCurrentUser() } returns user

    val request =
      mockRequest(
        mapOf("User-Agent" to "", "X-Forwarded-For" to TEST_VALUE_FORWARDED_FOR),
        "",
      )
    val result = auditLoggingHelper.buildActor(request.headers)
    val expected =
      Actor(
        userId.toString(),
        TEST_VALUE_USER_EMAIL,
        TEST_VALUE_FORWARDED_FOR,
        TEST_VALUE_UNKNOWN,
      )
    assertEquals(expected, result)
  }

  @Test
  fun `buildActor returns unknown if X-Forwarded-For is missing in headers`() {
    val user = AuthenticatedUser()
    val userId = UUID.randomUUID()

    user.userId = userId
    user.email = TEST_VALUE_USER_EMAIL

    every { currentUserService.getCurrentUser() } returns user

    val request =
      mockRequest(
        mapOf("User-Agent" to TEST_VALUE_USER_AGENT, "X-Forwarded-For" to ""),
        "",
      )

    val result = auditLoggingHelper.buildActor(request.headers)
    val expected =
      Actor(
        userId.toString(),
        TEST_VALUE_USER_EMAIL,
        TEST_VALUE_UNKNOWN,
        TEST_VALUE_USER_AGENT,
      )
    assertEquals(expected, result)
  }

  private fun mockRequest(
    headersMap: Map<String, String?>,
    rawBody: String,
  ): NettyHttpRequest<Any> {
    val request = mockk<NettyHttpRequest<Any>>(relaxed = true)
    every { request.contents().toString(any<Charset>()) } returns rawBody
    every { request.headers } answers {
      val headers = mockk<HttpHeaders>()
      headersMap.forEach { (key, value) ->
        every { headers.get(key) } returns value
      }
      headers
    }
    return request
  }

  @Nested
  inner class TestGenerateFinalSummary {
    @Test
    fun `should generate a final summary from request and result`() {
      val requestSummary =
        "{\"targetUser\":{\"id\":\"dummyId\",\"email\":null},\"targetScope\":{\"type\":\"organization\",\"id\":\"dummyId\"}," +
          "\"previousRole\":null,\"newRole\":\"organization_reader\"}"
      val resultSummary = "{\"result\": \"summary\"}"

      val finalSummary = auditLoggingHelper.generateSummary(requestSummary, resultSummary)

      assert(finalSummary.contains("result"))
      assert(finalSummary.contains("targetUser"))
      assert(finalSummary.contains("email"))
    }

    @Test
    fun `should generate a final summary from request and result with empty request`() {
      val requestSummary = "{}"
      val resultSummary = "{\"result\": \"summary\"}"

      val finalSummary = auditLoggingHelper.generateSummary(requestSummary, resultSummary)

      assert(finalSummary.contains("result"))
    }

    @Test
    fun `should generate a final summary from request and result with empty result`() {
      val requestSummary = "{}"
      val resultSummary = "{}"

      val finalSummary = auditLoggingHelper.generateSummary(requestSummary, resultSummary)

      assert(finalSummary == "{}")
    }
  }
}
