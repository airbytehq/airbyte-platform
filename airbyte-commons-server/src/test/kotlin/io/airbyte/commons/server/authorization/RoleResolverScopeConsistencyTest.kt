/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.auth.TokenType
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * End to end through [RoleResolver]: request refs -> [AuthenticationHeaderResolver] -> granted roles.
 *
 * The attacker is WORKSPACE_ADMIN of their own workspace and ORGANIZATION_ADMIN of their own organization and holds
 * nothing on the victim's. The reader holds only WORKSPACE_READER on the attacker's workspace.
 */
internal class RoleResolverScopeConsistencyTest {
  private val attackerAuthUserId = "attacker-auth-user"
  private val readerAuthUserId = "reader-auth-user"
  private val attackerWs = UUID.randomUUID()
  private val victimWs = UUID.randomUUID()
  private val attackerOrg = UUID.randomUUID()
  private val victimOrg = UUID.randomUUID()
  private val attackerSource = UUID.randomUUID()
  private val victimSource = UUID.randomUUID()
  private val attackerConn = UUID.randomUUID()
  private val victimConn = UUID.randomUUID()
  private val attackerJob = 4242L
  private val dataplaneServiceAccountId = UUID.randomUUID()

  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var roleResolver: RoleResolver

  @BeforeEach
  fun setup() {
    workspaceHelper = mockk()
    every { workspaceHelper.getWorkspaceForSourceId(attackerSource) } returns attackerWs
    every { workspaceHelper.getWorkspaceForSourceId(victimSource) } returns victimWs
    every { workspaceHelper.getWorkspaceForConnectionId(attackerConn) } returns attackerWs
    every { workspaceHelper.getWorkspaceForConnectionId(victimConn) } returns victimWs
    every { workspaceHelper.getWorkspaceForJobId(attackerJob) } returns attackerWs
    every { workspaceHelper.getOrganizationForWorkspace(attackerWs) } returns attackerOrg
    every { workspaceHelper.getOrganizationForWorkspace(victimWs) } returns victimOrg

    val permissionHandler = mockk<PermissionHandler>(relaxed = true)
    every { permissionHandler.getPermissionsByAuthUserId(attackerAuthUserId) } returns
      listOf(
        Permission().withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN).withWorkspaceId(attackerWs),
        Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(attackerOrg),
      )
    every { permissionHandler.getPermissionsByAuthUserId(readerAuthUserId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_READER).withWorkspaceId(attackerWs))
    every { permissionHandler.getPermissionsByServiceAccountId(dataplaneServiceAccountId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withOrganizationId(attackerOrg))

    val resolver =
      AuthenticationHeaderResolver(
        workspaceHelper,
        permissionHandler,
        mockk<UserPersistence>(relaxed = true),
        mockk<DataplaneGroupService>(relaxed = true),
        mockk<DataplaneService>(relaxed = true),
        mockk<MetricClient>(relaxed = true),
      )
    roleResolver = RoleResolver(resolver, mockk<CurrentUserService>(relaxed = true), null, permissionHandler)
  }

  private fun rolesFor(
    subject: String,
    vararg refs: Pair<AuthenticationId, String>,
  ): Set<String> = rolesFor(subject, TokenType.USER, refs.toList())

  private fun rolesFor(
    subject: String,
    type: TokenType,
    refs: List<Pair<AuthenticationId, String>>,
  ): Set<String> {
    val request = roleResolver.newRequest().withSubject(subject, type)
    refs.forEach { (key, value) -> request.withRef(key, value) }
    return request.roles()
  }

  /** A real denial still carries AUTHENTICATED_USER; an exception inside role resolution would yield an empty set. */
  private fun assertDeniedRoles(roles: Set<String>) {
    assertTrue(roles.contains(AuthRoleConstants.AUTHENTICATED_USER), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_READER), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.ORGANIZATION_READER), "roles=$roles")
  }

  @Test
  fun `victim workspaceId alone grants no workspace role`() {
    val roles = rolesFor(attackerAuthUserId, AuthenticationId.WORKSPACE_ID to victimWs.toString())
    assertDeniedRoles(roles)
  }

  @Test
  fun `victim organizationId alone grants no organization role`() {
    val roles = rolesFor(attackerAuthUserId, AuthenticationId.ORGANIZATION_ID to victimOrg.toString())
    assertDeniedRoles(roles)
  }

  @Test
  fun `victim sourceId with attacker workspaceId grants no role at all`() {
    val roles =
      rolesFor(
        attackerAuthUserId,
        AuthenticationId.SOURCE_ID to victimSource.toString(),
        AuthenticationId.WORKSPACE_ID to attackerWs.toString(),
      )
    assertDeniedRoles(roles)
  }

  @Test
  fun `victim workspaceId with attacker sourceId decoy grants no workspace role`() {
    val roles =
      rolesFor(
        attackerAuthUserId,
        AuthenticationId.WORKSPACE_ID to victimWs.toString(),
        AuthenticationId.SOURCE_ID to attackerSource.toString(),
      )
    assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_ADMIN), "roles=$roles")
    assertDeniedRoles(roles)
  }

  @Test
  fun `victim organizationId with attacker sourceId decoy grants no organization role`() {
    val roles =
      rolesFor(
        attackerAuthUserId,
        AuthenticationId.ORGANIZATION_ID to victimOrg.toString(),
        AuthenticationId.SOURCE_ID to attackerSource.toString(),
      )
    assertDeniedRoles(roles)
    assertFalse(roles.contains(AuthRoleConstants.ORGANIZATION_ADMIN), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_ADMIN), "roles=$roles")
  }

  @Test
  fun `victim sourceId with attacker connectionId decoy grants no workspace role`() {
    val roles =
      rolesFor(
        attackerAuthUserId,
        AuthenticationId.SOURCE_ID to victimSource.toString(),
        AuthenticationId.CONNECTION_ID to attackerConn.toString(),
      )
    assertDeniedRoles(roles)
  }

  @Test
  fun `withOrg does not grant scoped roles when request refs are rejected`() {
    val request =
      roleResolver
        .newRequest()
        .withSubject(attackerAuthUserId, TokenType.USER)
        .withOrg(attackerOrg)
        .withRef(AuthenticationId.SOURCE_ID, victimSource.toString())
        .withRef(AuthenticationId.WORKSPACE_ID, attackerWs.toString())

    val roles = request.roles()

    assertDeniedRoles(roles)
    assertFalse(roles.contains(AuthRoleConstants.ORGANIZATION_ADMIN), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_ADMIN), "roles=$roles")
  }

  @Test
  fun `own sourceId grants the caller's roles on their own workspace`() {
    val roles = rolesFor(attackerAuthUserId, AuthenticationId.SOURCE_ID to attackerSource.toString())
    assertTrue(roles.contains(AuthRoleConstants.WORKSPACE_ADMIN), "roles=$roles")
    assertTrue(roles.contains(AuthRoleConstants.ORGANIZATION_ADMIN), "roles=$roles")
  }

  @Test
  fun `reader in the same workspace does not gain editor or admin`() {
    val roles =
      rolesFor(
        readerAuthUserId,
        AuthenticationId.SOURCE_ID to attackerSource.toString(),
        AuthenticationId.WORKSPACE_ID to attackerWs.toString(),
      )
    assertTrue(roles.contains(AuthRoleConstants.WORKSPACE_READER), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_EDITOR), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.WORKSPACE_ADMIN), "roles=$roles")
  }

  @Test
  fun `listing two workspaces with an own sourceId decoy grants no role`() {
    val roles =
      rolesFor(
        attackerAuthUserId,
        AuthenticationId.WORKSPACE_IDS to serialize(listOf(victimWs.toString(), attackerWs.toString())),
        AuthenticationId.SOURCE_ID to attackerSource.toString(),
      )
    assertDeniedRoles(roles)
  }

  @Test
  fun `organization scoped dataplane keeps its role on a consistent request`() {
    val roles =
      rolesFor(
        dataplaneServiceAccountId.toString(),
        TokenType.DATAPLANE_V1,
        listOf(
          AuthenticationId.CONNECTION_ID to attackerConn.toString(),
          AuthenticationId.JOB_ID to attackerJob.toString(),
        ),
      )
    assertTrue(roles.contains(AuthRoleConstants.DATAPLANE), "roles=$roles")
  }

  @Test
  fun `organization scoped dataplane naming another organization's connection gets no role`() {
    val roles =
      rolesFor(
        dataplaneServiceAccountId.toString(),
        TokenType.DATAPLANE_V1,
        listOf(AuthenticationId.CONNECTION_ID to victimConn.toString()),
      )

    assertTrue(roles.contains(AuthRoleConstants.AUTHENTICATED_USER), "roles=$roles")
    assertFalse(roles.contains(AuthRoleConstants.DATAPLANE), "roles=$roles")
    verify(exactly = 1) { workspaceHelper.getWorkspaceForConnectionId(victimConn) }
    verify(exactly = 1) { workspaceHelper.getOrganizationForWorkspace(victimWs) }
  }
}
