/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONFIG_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.CONNECTION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_GROUP_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DATAPLANE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.DESTINATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.EXTERNAL_AUTH_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.IS_PUBLIC_API_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_ALT_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.JOB_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_SNAKE_CASE_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.PERMISSION_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SCOPE_TYPE_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.SOURCE_ID_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_IDS_HEADER
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.WORKSPACE_ID_HEADER
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.CompletionException

/**
 * Every identifier in a request must resolve to the same workspace and organization. An "attacker" owns one
 * workspace and organization; a "victim" owns another. Any pairing of a victim identifier with an attacker
 * identifier must resolve to no scope at all, regardless of which identifier the handler would act on.
 */
internal class AuthenticationHeaderResolverScopeTest {
  private val attackerWs = UUID.randomUUID()
  private val victimWs = UUID.randomUUID()
  private val attackerOrg = UUID.randomUUID()
  private val victimOrg = UUID.randomUUID()
  private val attackerSource = UUID.randomUUID()
  private val victimSource = UUID.randomUUID()
  private val attackerConn = UUID.randomUUID()
  private val attackerConn2 = UUID.randomUUID()
  private val victimConn = UUID.randomUUID()
  private val attackerDest = UUID.randomUUID()
  private val victimDest = UUID.randomUUID()
  private val attackerJob = 4242L
  private val victimJob = 7L

  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var dataplaneGroupService: DataplaneGroupService
  private lateinit var dataplaneService: DataplaneService
  private lateinit var metricClient: MetricClient
  private lateinit var resolver: AuthenticationHeaderResolver

  @BeforeEach
  fun setup() {
    workspaceHelper = mockk()
    every { workspaceHelper.getWorkspaceForSourceId(attackerSource) } returns attackerWs
    every { workspaceHelper.getWorkspaceForSourceId(victimSource) } returns victimWs
    every { workspaceHelper.getWorkspaceForDestinationId(attackerDest) } returns attackerWs
    every { workspaceHelper.getWorkspaceForDestinationId(victimDest) } returns victimWs
    every { workspaceHelper.getWorkspaceForConnectionId(attackerConn) } returns attackerWs
    every { workspaceHelper.getWorkspaceForConnectionId(attackerConn2) } returns attackerWs
    every { workspaceHelper.getWorkspaceForConnectionId(victimConn) } returns victimWs
    every { workspaceHelper.getWorkspaceForJobId(attackerJob) } returns attackerWs
    every { workspaceHelper.getWorkspaceForJobId(victimJob) } returns victimWs
    every { workspaceHelper.getWorkspaceForConnection(victimSource, attackerDest) } throws
      IllegalArgumentException("Source and destination must be from the same workspace!")
    every { workspaceHelper.getOrganizationForWorkspace(attackerWs) } returns attackerOrg
    every { workspaceHelper.getOrganizationForWorkspace(victimWs) } returns victimOrg

    permissionHandler = mockk()
    dataplaneGroupService = mockk()
    dataplaneService = mockk()
    metricClient = mockk(relaxed = true)

    resolver =
      AuthenticationHeaderResolver(
        workspaceHelper,
        permissionHandler,
        mockk<UserPersistence>(relaxed = true),
        dataplaneGroupService,
        dataplaneService,
        metricClient,
      )
  }

  private fun assertDenied(reason: String) {
    verify(exactly = 1) {
      metricClient.count(OssMetricsRegistry.AUTHORIZATION_SCOPE_CONFLICT, 1L, MetricAttribute(MetricTags.FAILURE_TYPE, reason))
    }
  }

  private fun assertNotDenied() {
    verify(exactly = 0) { metricClient.count(any(), any(), *anyVararg()) }
  }

  // --- the CVE and both directions of decoy ---

  @Test
  fun `victim sourceId with attacker workspaceId is denied`() {
    val props = mapOf(SOURCE_ID_HEADER to victimSource.toString(), WORKSPACE_ID_HEADER to attackerWs.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `victim workspaceId with attacker sourceId decoy is denied`() {
    val props = mapOf(WORKSPACE_ID_HEADER to victimWs.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `victim organizationId with attacker sourceId decoy is denied`() {
    val props = mapOf(ORGANIZATION_ID_HEADER to victimOrg.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_workspace_mismatch")
  }

  @Test
  fun `both public methods return null on a denied request`() {
    val props = mapOf(ORGANIZATION_ID_HEADER to victimOrg.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertNull(resolver.resolveWorkspace(props))
    assertNull(resolver.resolveOrganization(props))
  }

  @Test
  fun `victim sourceId with attacker connectionId decoy is denied`() {
    val props = mapOf(SOURCE_ID_HEADER to victimSource.toString(), CONNECTION_ID_HEADER to attackerConn.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `victim sourceId with attacker jobId decoy is denied`() {
    val props = mapOf(SOURCE_ID_HEADER to victimSource.toString(), JOB_ID_HEADER to attackerJob.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `victim destinationId with attacker connectionId decoy is denied`() {
    val props = mapOf(DESTINATION_ID_HEADER to victimDest.toString(), CONNECTION_ID_HEADER to attackerConn.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `victim sourceId with attacker destinationId is denied as unresolvable`() {
    val props = mapOf(SOURCE_ID_HEADER to victimSource.toString(), DESTINATION_ID_HEADER to attackerDest.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_ref")
  }

  @Test
  fun `victim connectionIds with attacker workspaceId are denied`() {
    val props =
      mapOf(
        WORKSPACE_ID_HEADER to attackerWs.toString(),
        CONNECTION_IDS_HEADER to serialize(listOf(attackerConn.toString(), victimConn.toString())),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("connection_ids_disagree")
  }

  @Test
  fun `configId outside workspaceIds is denied`() {
    val props =
      mapOf(
        WORKSPACE_IDS_HEADER to serialize(listOf(victimWs.toString())),
        CONFIG_ID_HEADER to attackerConn.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("outside_workspace_ids")
  }

  @Test
  fun `two different organization claims are denied`() {
    val props =
      mapOf(
        ORGANIZATION_ID_HEADER to attackerOrg.toString(),
        SCOPE_TYPE_HEADER to ScopeType.ORGANIZATION.value(),
        SCOPE_ID_HEADER to victimOrg.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_refs_disagree")
  }

  @Test
  fun `dataplane group organization must match the resource organization`() {
    val dataplaneGroupId = UUID.randomUUID()
    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplaneGroupId) } returns victimOrg
    val props = mapOf(DATAPLANE_GROUP_ID_HEADER to dataplaneGroupId.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_workspace_mismatch")
  }

  @Test
  fun `permission scoped to a workspace in another organization than the claim is denied`() {
    val permissionId = UUID.randomUUID()
    every { permissionHandler.getPermissionById(permissionId) } returns Permission().withWorkspaceId(attackerWs)
    val props = mapOf(PERMISSION_ID_HEADER to permissionId.toString(), ORGANIZATION_ID_HEADER to victimOrg.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_workspace_mismatch")
  }

  // --- honest requests keep working ---

  @Test
  fun `matching workspaceId and sourceId resolve to that workspace and its organization`() {
    val props = mapOf(WORKSPACE_ID_HEADER to attackerWs.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    val scope = resolver.resolveScope(props)
    assertEquals(listOf(attackerWs), scope?.workspaceIds)
    assertEquals(listOf(attackerOrg), scope?.organizationIds)
    assertEquals(mapOf(attackerWs to attackerOrg), scope?.workspaceOrganizations)
    assertNotDenied()
  }

  @Test
  fun `connectionIds in the same workspace as workspaceId resolve to that workspace`() {
    val props =
      mapOf(
        WORKSPACE_ID_HEADER to attackerWs.toString(),
        CONNECTION_IDS_HEADER to serialize(listOf(attackerConn.toString(), attackerConn2.toString())),
      )
    assertEquals(listOf(attackerWs), resolver.resolveWorkspace(props))
    assertNotDenied()
  }

  @Test
  fun `configId inside workspaceIds resolves to every listed workspace`() {
    val props =
      mapOf(
        WORKSPACE_IDS_HEADER to serialize(listOf(attackerWs.toString(), victimWs.toString())),
        CONFIG_ID_HEADER to attackerConn.toString(),
      )
    assertEquals(listOf(attackerWs, victimWs), resolver.resolveWorkspace(props))
    assertNotDenied()
  }

  @Test
  fun `organization claim matching the workspace organization resolves to that organization`() {
    val props = mapOf(ORGANIZATION_ID_HEADER to attackerOrg.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertEquals(listOf(attackerOrg), resolver.resolveOrganization(props))
    assertNotDenied()
  }

  @Test
  fun `trusted workspace organization is used instead of the cache`() {
    val freshOrg = UUID.randomUUID()
    val props = mapOf(WORKSPACE_ID_HEADER to attackerWs.toString(), ORGANIZATION_ID_HEADER to freshOrg.toString())
    val scope = resolver.resolveScope(props, mapOf(attackerWs to freshOrg))
    assertEquals(listOf(attackerWs), scope?.workspaceIds)
    assertEquals(listOf(freshOrg), scope?.organizationIds)
    verify(exactly = 0) { workspaceHelper.getOrganizationForWorkspace(any()) }
    assertNotDenied()
  }

  @Test
  fun `uuid id field is not a job id and does not suppress organization resolution`() {
    val props = mapOf(JOB_ID_HEADER to UUID.randomUUID().toString(), ORGANIZATION_ID_HEADER to victimOrg.toString())
    assertNull(resolver.resolveWorkspace(props))
    assertEquals(listOf(victimOrg), resolver.resolveOrganization(props))
    assertNotDenied()
  }

  @Test
  fun `a scope type that is not known next to a scope id is denied`() {
    // An endpoint can read a number as the position of a scope type, so the scope id must not be skipped.
    val props =
      mapOf(
        SCOPE_TYPE_HEADER to "1",
        SCOPE_ID_HEADER to victimWs.toString(),
        WORKSPACE_ID_HEADER to attackerWs.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_ref")
  }

  @Test
  fun `a permission id that is not a uuid is denied`() {
    // An endpoint can read the Base64 form of a UUID, so a permission id in that form must not be skipped.
    val props = mapOf(PERMISSION_ID_HEADER to "AAECAwQFBgcICQoLDA0ODw==", WORKSPACE_ID_HEADER to attackerWs.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_ref")
  }

  @Test
  fun `a jobId field that hides the job id of a victim is denied`() {
    // The endpoint reads `id`. A `jobId` field that the endpoint ignores must not replace it in the permission check.
    val props =
      mapOf(
        JOB_ID_HEADER to victimJob.toString(),
        JOB_ID_ALT_HEADER to attackerJob.toString(),
        WORKSPACE_ID_HEADER to attackerWs.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `a job id that is not a whole number is denied`() {
    // The endpoint reads 7.0 as job 7, so the permission check must not skip it.
    val props = mapOf(JOB_ID_HEADER to "7.0", WORKSPACE_ID_HEADER to attackerWs.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_ref")
  }

  @Test
  fun `a jobId field resolves to the workspace of the job`() {
    val scope = resolver.resolveScope(mapOf(JOB_ID_ALT_HEADER to attackerJob.toString()))
    assertEquals(listOf(attackerWs), scope?.workspaceIds)
    assertNotDenied()
  }

  @Test
  fun `a uuid id next to a jobId resolves to the workspace of the job`() {
    val props = mapOf(JOB_ID_HEADER to UUID.randomUUID().toString(), JOB_ID_ALT_HEADER to attackerJob.toString())
    assertEquals(listOf(attackerWs), resolver.resolveScope(props)?.workspaceIds)
    assertNotDenied()
  }

  @Test
  fun `an organization_id field that hides the organization of a victim is denied`() {
    // The endpoint reads `organizationId`. The other spelling must not replace it in the permission check.
    val props =
      mapOf(
        ORGANIZATION_ID_HEADER to victimOrg.toString(),
        ORGANIZATION_ID_SNAKE_CASE_HEADER to attackerOrg.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_refs_disagree")
  }

  @Test
  fun `both spellings of the organization id that agree resolve to that organization`() {
    val props =
      mapOf(
        ORGANIZATION_ID_HEADER to attackerOrg.toString(),
        ORGANIZATION_ID_SNAKE_CASE_HEADER to attackerOrg.toString(),
      )
    assertEquals(listOf(attackerOrg), resolver.resolveScope(props)?.organizationIds)
    assertNotDenied()
  }

  @Test
  fun `permission scoped to an organization resolves that organization`() {
    val permissionId = UUID.randomUUID()
    every { permissionHandler.getPermissionById(permissionId) } returns Permission().withOrganizationId(victimOrg)
    val props = mapOf(PERMISSION_ID_HEADER to permissionId.toString())
    assertNull(resolver.resolveWorkspace(props))
    assertEquals(listOf(victimOrg), resolver.resolveOrganization(props))
    assertNotDenied()
  }

  @Test
  fun `public api request without workspace refs passes through as an empty list`() {
    val props = mapOf(IS_PUBLIC_API_HEADER to "true")
    assertEquals(emptyList<UUID>(), resolver.resolveWorkspace(props))
    assertNull(resolver.resolveOrganization(props))
    assertNotDenied()
  }

  // --- review follow-ups ---

  @Test
  fun `wrappers resolve silently and only resolveScope reports`() {
    val props = mapOf(WORKSPACE_ID_HEADER to victimWs.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertNull(resolver.resolveWorkspace(props))
    assertNull(resolver.resolveOrganization(props))
    assertNotDenied()
    assertNull(resolver.resolveScope(props))
    assertDenied("workspace_refs_disagree")
  }

  @Test
  fun `connectionIds inside workspaceIds resolve to every listed workspace`() {
    val props =
      mapOf(
        WORKSPACE_IDS_HEADER to serialize(listOf(attackerWs.toString(), victimWs.toString())),
        CONNECTION_IDS_HEADER to serialize(listOf(attackerConn.toString(), victimConn.toString())),
      )
    assertEquals(listOf(attackerWs, victimWs), resolver.resolveWorkspace(props))
    assertNotDenied()
  }

  @Test
  fun `connectionIds outside workspaceIds are denied`() {
    val props =
      mapOf(
        WORKSPACE_IDS_HEADER to serialize(listOf(attackerWs.toString())),
        CONNECTION_IDS_HEADER to serialize(listOf(victimConn.toString())),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("outside_workspace_ids")
  }

  @Test
  fun `connectionIds alone across two workspaces resolve to both`() {
    val props = mapOf(CONNECTION_IDS_HEADER to serialize(listOf(attackerConn.toString(), victimConn.toString())))
    assertEquals(listOf(attackerWs, victimWs), resolver.resolveWorkspace(props))
    assertNotDenied()
  }

  @Test
  fun `more than 1000 connectionIds are denied before connection lookup`() {
    val props = mapOf(CONNECTION_IDS_HEADER to serialize(List(1001) { attackerConn.toString() }))

    assertNull(resolver.resolveScope(props))

    verify(exactly = 0) { workspaceHelper.getWorkspaceForConnectionId(any()) }
    assertDenied("too_many_connection_ids")
  }

  @Test
  fun `more than 1000 workspaceIds are denied before organization lookup`() {
    val props = mapOf(WORKSPACE_IDS_HEADER to serialize(List(1001) { attackerWs.toString() }))

    assertNull(resolver.resolveScope(props))

    verify(exactly = 0) { workspaceHelper.getOrganizationForWorkspace(any()) }
    assertDenied("too_many_workspace_ids")
  }

  @Test
  fun `denial warning contains field names but no client supplied values`() {
    val sensitiveUserId = "sensitive-user-id"
    val props =
      mapOf(
        ORGANIZATION_ID_HEADER to attackerOrg.toString(),
        ORGANIZATION_ID_SNAKE_CASE_HEADER to victimOrg.toString(),
        CONNECTION_IDS_HEADER to serialize(List(10) { attackerConn.toString() }),
        EXTERNAL_AUTH_ID_HEADER to sensitiveUserId,
      )
    val logger = LoggerFactory.getLogger(AuthenticationHeaderResolver::class.java) as Logger
    val appender =
      ListAppender<ILoggingEvent>().apply {
        context = logger.loggerContext
        start()
      }
    logger.addAppender(appender)

    try {
      assertNull(resolver.resolveScope(props))
    } finally {
      logger.detachAppender(appender)
      appender.stop()
    }

    val warning = appender.list.single { it.level.levelStr == "WARN" }.formattedMessage
    assertTrue(warning.contains("organization_refs_disagree"), warning)
    assertTrue(warning.contains(ORGANIZATION_ID_HEADER), warning)
    assertTrue(warning.contains(ORGANIZATION_ID_SNAKE_CASE_HEADER), warning)
    assertTrue(warning.contains(CONNECTION_IDS_HEADER), warning)
    assertTrue(warning.contains(EXTERNAL_AUTH_ID_HEADER), warning)
    assertFalse(warning.contains(attackerOrg.toString()), warning)
    assertFalse(warning.contains(victimOrg.toString()), warning)
    assertFalse(warning.contains(attackerConn.toString()), warning)
    assertFalse(warning.contains(sensitiveUserId), warning)
    assertTrue(warning.length < 1000, warning)
  }

  @Test
  fun `duplicate connectionIds are looked up once`() {
    val props = mapOf(CONNECTION_IDS_HEADER to serialize(listOf(attackerConn.toString(), attackerConn.toString())))

    assertEquals(listOf(attackerWs), resolver.resolveWorkspace(props))

    verify(exactly = 1) { workspaceHelper.getWorkspaceForConnectionId(attackerConn) }
    assertNotDenied()
  }

  @Test
  fun `workspace scopeId with a mismatching organization claim is denied`() {
    val props =
      mapOf(
        SCOPE_TYPE_HEADER to ScopeType.WORKSPACE.value(),
        SCOPE_ID_HEADER to attackerWs.toString(),
        ORGANIZATION_ID_HEADER to victimOrg.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_workspace_mismatch")
  }

  @Test
  fun `listed workspaces spanning two organizations with an organization claim are denied`() {
    val props =
      mapOf(
        WORKSPACE_IDS_HEADER to serialize(listOf(attackerWs.toString(), victimWs.toString())),
        ORGANIZATION_ID_HEADER to attackerOrg.toString(),
      )
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_workspace_mismatch")
  }

  @Test
  fun `a single ref that is not found is denied as unresolvable`() {
    val missingSource = UUID.randomUUID()
    every { workspaceHelper.getWorkspaceForSourceId(missingSource) } throws ConfigNotFoundException("source", missingSource.toString())
    val props = mapOf(SOURCE_ID_HEADER to missingSource.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_ref")
  }

  @Test
  fun `an unknown workspace under an organization claim is denied`() {
    // WorkspaceHelper wraps the cache loader's failure twice before it reaches the resolver.
    every { workspaceHelper.getOrganizationForWorkspace(attackerWs) } throws
      RuntimeException(CompletionException(ConfigNotFoundException("workspace", attackerWs.toString())))
    val props = mapOf(WORKSPACE_ID_HEADER to attackerWs.toString(), ORGANIZATION_ID_HEADER to attackerOrg.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_workspace_organization")
  }

  @Test
  fun `a failing organization lookup is not an authorization decision`() {
    every { workspaceHelper.getOrganizationForWorkspace(attackerWs) } throws RuntimeException("db down")
    val props = mapOf(WORKSPACE_ID_HEADER to attackerWs.toString(), ORGANIZATION_ID_HEADER to attackerOrg.toString())
    assertThrows<RuntimeException> { resolver.resolveScope(props) }
    verify { metricClient wasNot Called }
  }

  @Test
  fun `a failing workspace lookup is not an authorization decision`() {
    every { workspaceHelper.getWorkspaceForSourceId(attackerSource) } throws RuntimeException("db down")
    val props = mapOf(SOURCE_ID_HEADER to attackerSource.toString())
    assertThrows<RuntimeException> { resolver.resolveScope(props) }
    verify { metricClient wasNot Called }
  }

  @Test
  fun `a dataplane that is not found is denied`() {
    val dataplaneId = UUID.randomUUID()
    every { dataplaneService.getDataplane(dataplaneId) } throws RuntimeException("Dataplane not found: $dataplaneId")
    val props = mapOf(DATAPLANE_ID_HEADER to dataplaneId.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("unresolvable_dataplane")
  }

  @Test
  fun `a shared dataplane group with a named organization adds no organization claim`() {
    val sharedGroupId = UUID.randomUUID()
    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(sharedGroupId) } returns DEFAULT_ORGANIZATION_ID
    val props =
      mapOf(
        DATAPLANE_GROUP_ID_HEADER to sharedGroupId.toString(),
        ORGANIZATION_ID_HEADER to attackerOrg.toString(),
        SOURCE_ID_HEADER to attackerSource.toString(),
      )
    assertEquals(listOf(attackerOrg), resolver.resolveOrganization(props))
    assertNotDenied()
  }

  @Test
  fun `a PrivateLink dataplane group with a named organization adds no organization claim`() {
    val privateLinkGroupId = UUID.randomUUID()
    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(privateLinkGroupId) } returns
      PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
    val props = mapOf(DATAPLANE_GROUP_ID_HEADER to privateLinkGroupId.toString(), ORGANIZATION_ID_HEADER to attackerOrg.toString())
    assertEquals(listOf(attackerOrg), resolver.resolveOrganization(props))
    assertNotDenied()
  }

  @Test
  fun `a shared dataplane group without a named organization is the scope`() {
    val sharedGroupId = UUID.randomUUID()
    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(sharedGroupId) } returns DEFAULT_ORGANIZATION_ID
    val props = mapOf(DATAPLANE_GROUP_ID_HEADER to sharedGroupId.toString(), SOURCE_ID_HEADER to attackerSource.toString())
    assertNull(resolver.resolveScope(props))
    assertDenied("organization_workspace_mismatch")
  }

  @Test
  fun `a permission id that does not resolve is ignored`() {
    val permissionId = UUID.randomUUID()
    every { permissionHandler.getPermissionById(permissionId) } throws ConfigNotFoundException("permission", permissionId.toString())
    val props = mapOf(PERMISSION_ID_HEADER to permissionId.toString(), WORKSPACE_ID_HEADER to attackerWs.toString())
    assertEquals(listOf(attackerWs), resolver.resolveWorkspace(props))
    assertNotDenied()
  }
}
