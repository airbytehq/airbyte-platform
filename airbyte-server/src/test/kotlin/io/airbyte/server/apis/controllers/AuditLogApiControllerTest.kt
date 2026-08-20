/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.AuditLogListRequestBody
import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.audit.logging.read.AuditLogPage
import io.airbyte.audit.logging.read.AuditLogQuery
import io.airbyte.audit.logging.read.AuditLogReadService
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.micronaut.http.HttpHeaders
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.server.netty.NettyHttpRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID

class AuditLogApiControllerTest {
  private lateinit var auditLogReadService: AuditLogReadService
  private lateinit var authenticationHeaderResolver: AuthenticationHeaderResolver
  private lateinit var controller: AuditLogApiController

  @BeforeEach
  fun setUp() {
    auditLogReadService = mockk()
    authenticationHeaderResolver = mockk()
    controller = AuditLogApiController(auditLogReadService, authenticationHeaderResolver)

    mockkStatic(ServerRequestContext::class)
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `rejects a request body naming an organization other than the authorized one`() {
    val authorizedOrganizationId = UUID.randomUUID()
    stubRequestOrganization(authorizedOrganizationId)

    val body = AuditLogListRequestBody(organizationId = UUID.randomUUID())

    assertThrows<ForbiddenProblem> { controller.listAuditLogs(body) }
    verify(exactly = 0) { auditLogReadService.listAuditLogs(any()) }
  }

  @Test
  fun `rejects the request when no organization can be resolved from the request context`() {
    stubRequestOrganization(null)

    val body = AuditLogListRequestBody(organizationId = UUID.randomUUID())

    assertThrows<ForbiddenProblem> { controller.listAuditLogs(body) }
    verify(exactly = 0) { auditLogReadService.listAuditLogs(any()) }
  }

  @Test
  fun `maps the service page to the api response`() {
    val organizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    stubRequestOrganization(organizationId)

    val entry =
      AuditLogEntry(
        id = UUID.randomUUID(),
        timestamp = 1755800000000,
        actor = Actor(actorId = "user-1", email = "user-1@airbyte.io", ipAddress = "10.0.0.1", userAgent = "browser"),
        operation = "updateSsoConfig",
        request = "{\"redacted\":true}",
        response = null,
        success = true,
        errorMessage = null,
        organizationId = organizationId,
        workspaceId = workspaceId,
      )
    every { auditLogReadService.listAuditLogs(any()) } returns AuditLogPage(entries = listOf(entry), nextPageToken = "next-token")

    val result = controller.listAuditLogs(AuditLogListRequestBody(organizationId = organizationId))

    assertEquals("next-token", result.nextPageToken)
    val read = result.auditLogs.single()
    assertEquals(entry.id, read.id)
    assertEquals(entry.timestamp, read.timestamp)
    assertEquals("updateSsoConfig", read.operation)
    assertEquals(true, read.success)
    assertEquals(organizationId, read.organizationId)
    assertEquals(workspaceId, read.workspaceId)
    assertEquals("user-1", read.actor?.actorId)
    assertEquals("user-1@airbyte.io", read.actor?.email)
    assertEquals("10.0.0.1", read.actor?.ipAddress)
    assertEquals("browser", read.actor?.userAgent)
    assertEquals(true, read.request?.get("redacted")?.asBoolean())
    assertNull(read.response)
    assertNull(read.errorMessage)
  }

  @Test
  fun `passes all filters through to the service query`() {
    val organizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    stubRequestOrganization(organizationId)

    val queries = mutableListOf<AuditLogQuery>()
    every { auditLogReadService.listAuditLogs(capture(queries)) } returns AuditLogPage(entries = emptyList())

    val body =
      AuditLogListRequestBody(
        organizationId = organizationId,
        startTime = OffsetDateTime.of(2026, 8, 1, 0, 0, 0, 0, ZoneOffset.UTC),
        endTime = OffsetDateTime.of(2026, 8, 14, 0, 0, 0, 0, ZoneOffset.UTC),
        workspaceId = workspaceId,
        actorId = "user-1",
        operation = "updateConnection",
        success = false,
        searchText = "sso",
        pageSize = 25,
        pageToken = "some-token",
      )

    controller.listAuditLogs(body)

    val query = queries.single()
    assertEquals(organizationId, query.organizationId)
    assertEquals(body.startTime?.toInstant(), query.startTime)
    assertEquals(body.endTime?.toInstant(), query.endTime)
    assertEquals(workspaceId, query.workspaceId)
    assertEquals("user-1", query.actorId)
    assertEquals("updateConnection", query.operation)
    assertEquals(false, query.success)
    assertEquals("sso", query.searchText)
    assertEquals(25, query.pageSize)
    assertEquals("some-token", query.pageToken)
  }

  @Test
  fun `maps an invalid query to a bad request`() {
    val organizationId = UUID.randomUUID()
    stubRequestOrganization(organizationId)
    every { auditLogReadService.listAuditLogs(any()) } throws IllegalArgumentException("Page token references an unknown file.")

    assertThrows<BadRequestException> {
      controller.listAuditLogs(AuditLogListRequestBody(organizationId = organizationId))
    }
  }

  @Test
  fun `non-json request summaries are returned as text nodes`() {
    val organizationId = UUID.randomUUID()
    stubRequestOrganization(organizationId)

    val entry =
      AuditLogEntry(
        id = UUID.randomUUID(),
        timestamp = 1755800000000,
        operation = "deleteConnection",
        request = "not json",
        success = false,
        errorMessage = "boom",
        organizationId = organizationId,
        workspaceId = null,
      )
    every { auditLogReadService.listAuditLogs(any()) } returns AuditLogPage(entries = listOf(entry))

    val result = controller.listAuditLogs(AuditLogListRequestBody(organizationId = organizationId))

    val read = result.auditLogs.single()
    assertTrue(read.request?.isTextual == true)
    assertEquals("not json", read.request?.asText())
    assertEquals("boom", read.errorMessage)
    assertNull(read.workspaceId)
    assertNull(result.nextPageToken)
  }

  private fun stubRequestOrganization(organizationId: UUID?) {
    val request = mockk<NettyHttpRequest<Any>>()
    val headers = mockk<HttpHeaders>()
    val headerValues = organizationId?.let { mapOf(ORGANIZATION_ID_HEADER to it.toString()) } ?: emptyMap()
    every { request.headers } returns headers
    every { headers.asMap(String::class.java, String::class.java) } returns headerValues
    every { ServerRequestContext.currentRequest<Any>() } returns Optional.of(request)
    every { authenticationHeaderResolver.resolveOrganization(headerValues) } returns organizationId?.let { listOf(it) }
  }
}
