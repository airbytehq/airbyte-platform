/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.mockk.mockk
import io.mockk.unmockkAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

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
