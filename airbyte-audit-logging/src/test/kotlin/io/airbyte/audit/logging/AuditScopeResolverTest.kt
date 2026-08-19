/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.data.helpers.WorkspaceHelper
import io.micronaut.http.HttpHeaders
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class AuditScopeResolverTest {
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var resolver: AuditScopeResolver

  @BeforeEach
  fun setUp() {
    workspaceHelper = mockk()
    resolver = AuditScopeResolver(workspaceHelper)
  }

  @Test
  fun `resolves workspace and organization from headers`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val headers = headersWith(WORKSPACE_ID_HEADER to workspaceId.toString(), ORGANIZATION_ID_HEADER to organizationId.toString())

    val scope = resolver.resolveScope(headers, null, null)

    assertEquals(workspaceId, scope.workspaceId)
    assertEquals(organizationId, scope.organizationId)
  }

  @Test
  fun `organization header wins over body and workspace-derived organization`() {
    val headerOrganizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val headers = headersWith(ORGANIZATION_ID_HEADER to headerOrganizationId.toString())
    val requestBody = mapOf("organizationId" to UUID.randomUUID().toString(), "workspaceId" to workspaceId.toString())

    val scope = resolver.resolveScope(headers, requestBody, null)

    assertEquals(headerOrganizationId, scope.organizationId)
    assertEquals(workspaceId, scope.workspaceId)
  }

  @Test
  fun `resolves workspaceId from request body and organization from workspace`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId

    val scope = resolver.resolveScope(null, mapOf("workspaceId" to workspaceId.toString()), null)

    assertEquals(workspaceId, scope.workspaceId)
    assertEquals(organizationId, scope.organizationId)
  }

  @Test
  fun `resolves workspaceId from response body when request body lacks it`() {
    val workspaceId = UUID.randomUUID()
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns UUID.randomUUID()

    val scope = resolver.resolveScope(null, mapOf("name" to "no ids here"), mapOf("workspaceId" to workspaceId.toString()))

    assertEquals(workspaceId, scope.workspaceId)
  }

  @Test
  fun `request body workspaceId wins over response body workspaceId`() {
    val requestWorkspaceId = UUID.randomUUID()
    val responseWorkspaceId = UUID.randomUUID()
    every { workspaceHelper.getOrganizationForWorkspace(requestWorkspaceId) } returns UUID.randomUUID()

    val scope =
      resolver.resolveScope(
        null,
        mapOf("workspaceId" to requestWorkspaceId.toString()),
        mapOf("workspaceId" to responseWorkspaceId.toString()),
      )

    assertEquals(requestWorkspaceId, scope.workspaceId)
  }

  @Test
  fun `resolves workspaceId from connectionId resource lookup`() {
    val connectionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every { workspaceHelper.getWorkspaceForConnectionId(connectionId) } returns workspaceId
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId

    val scope = resolver.resolveScope(null, mapOf("connectionId" to connectionId.toString()), null)

    assertEquals(workspaceId, scope.workspaceId)
    assertEquals(organizationId, scope.organizationId)
  }

  @Test
  fun `falls through to sourceId when connectionId lookup fails`() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { workspaceHelper.getWorkspaceForConnectionId(connectionId) } throws RuntimeException("not found")
    every { workspaceHelper.getWorkspaceForSourceId(sourceId) } returns workspaceId
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns UUID.randomUUID()

    val scope =
      resolver.resolveScope(
        null,
        mapOf("connectionId" to connectionId.toString(), "sourceId" to sourceId.toString()),
        null,
      )

    assertEquals(workspaceId, scope.workspaceId)
  }

  @Test
  fun `resolves workspaceId from destinationId resource lookup`() {
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { workspaceHelper.getWorkspaceForDestinationId(destinationId) } returns workspaceId
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns UUID.randomUUID()

    val scope = resolver.resolveScope(null, mapOf("destinationId" to destinationId.toString()), null)

    assertEquals(workspaceId, scope.workspaceId)
  }

  @Test
  fun `resolves workspaceId from response body resource lookup when request body has none`() {
    val connectionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { workspaceHelper.getWorkspaceForConnectionId(connectionId) } returns workspaceId
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns UUID.randomUUID()

    val scope = resolver.resolveScope(null, mapOf("name" to "nothing"), mapOf("connectionId" to connectionId.toString()))

    assertEquals(workspaceId, scope.workspaceId)
  }

  @Test
  fun `resolves organizationId from body field for org-level action with null workspace`() {
    val organizationId = UUID.randomUUID()

    val scope = resolver.resolveScope(null, mapOf("organizationId" to organizationId.toString()), null)

    assertEquals(organizationId, scope.organizationId)
    assertNull(scope.workspaceId)
  }

  @Test
  fun `returns nulls when nothing resolves`() {
    val scope = resolver.resolveScope(null, mapOf("name" to "unrelated"), null)

    assertNull(scope.workspaceId)
    assertNull(scope.organizationId)
  }

  @Test
  fun `handles null headers and null bodies`() {
    val scope = resolver.resolveScope(null, null, null)

    assertNull(scope.workspaceId)
    assertNull(scope.organizationId)
  }

  @Test
  fun `ignores unparseable header value and falls through to body`() {
    val workspaceId = UUID.randomUUID()
    val headers = headersWith(WORKSPACE_ID_HEADER to "not-a-uuid")
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns UUID.randomUUID()

    val scope = resolver.resolveScope(headers, mapOf("workspaceId" to workspaceId.toString()), null)

    assertEquals(workspaceId, scope.workspaceId)
  }

  @Test
  fun `returns null organization when workspace organization lookup fails`() {
    val workspaceId = UUID.randomUUID()
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } throws RuntimeException("db down")

    val scope = resolver.resolveScope(null, mapOf("workspaceId" to workspaceId.toString()), null)

    assertEquals(workspaceId, scope.workspaceId)
    assertNull(scope.organizationId)
  }

  private fun headersWith(vararg entries: Pair<String, String>): HttpHeaders {
    val headers = mockk<HttpHeaders>()
    every { headers.get(WORKSPACE_ID_HEADER) } returns null
    every { headers.get(ORGANIZATION_ID_HEADER) } returns null
    entries.forEach { (name, value) -> every { headers.get(name) } returns value }
    return headers
  }
}
