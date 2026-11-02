/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class PublicEmbeddedCompatibilityTest {
  @Test
  fun `embedded destination creation keeps its scoped workspace action`() {
    val fixture = Fixture()
    every { fixture.grants.getPermissionsByAuthUserId("caller") } returns emptyList()
    every { fixture.data.workspaceAndOrganization(WORKSPACE) } returns OWNER
    every { fixture.data.requireActorDefinitionType(DEFINITION, false) } just Runs
    every { fixture.data.canUseActorDefinition(DEFINITION, OWNER) } returns true
    val rule = PublicDestinationCreateRule(fixture.data, fixture.permissions)
    val input = PublicActorCreateInput(WORKSPACE, DEFINITION)

    assertDoesNotThrow { rule.authorize(embedded(WORKSPACE), input) }
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(embedded(OTHER_WORKSPACE), input) }
  }

  @Test
  fun `embedded job actions keep issuer grants inside the token workspace`() {
    val fixture = Fixture()
    fixture.grant(Permission.PermissionType.WORKSPACE_EDITOR)
    val create = PublicJobCreateRule(fixture.data, fixture.permissions)
    val read = PublicJobReadRule(fixture.data, fixture.permissions)
    val cancel = PublicJobCancelRule(fixture.data, fixture.permissions)
    val list = PublicJobListRule(fixture.data, fixture.permissions)
    val caller = embedded(WORKSPACE)

    assertDoesNotThrow { create.authorize(caller, PublicJobCreateInput(CONNECTION, false)) }
    assertDoesNotThrow { create.authorize(caller, PublicJobCreateInput(CONNECTION, true)) }
    assertDoesNotThrow { read.authorize(caller, PublicJobInput(1L)) }
    assertDoesNotThrow { cancel.authorize(caller, PublicJobInput(1L)) }
    assertDoesNotThrow { list.authorize(caller, PublicJobsListInput(Optional.of(CONNECTION), emptyList())) }
    assertDoesNotThrow { list.authorize(caller, PublicJobsListInput(Optional.empty(), listOf(WORKSPACE))) }

    assertThrows(ForbiddenProblem::class.java) { create.authorize(embedded(OTHER_WORKSPACE), PublicJobCreateInput(CONNECTION, false)) }
    assertThrows(ForbiddenProblem::class.java) { list.authorize(caller, PublicJobsListInput(Optional.empty(), emptyList())) }
  }

  @Test
  fun `embedded job start still needs the issuing user's runner grant`() {
    val fixture = Fixture()
    fixture.grant(Permission.PermissionType.WORKSPACE_READER)
    val rule = PublicJobCreateRule(fixture.data, fixture.permissions)

    assertThrows(ForbiddenProblem::class.java) { rule.authorize(embedded(WORKSPACE), PublicJobCreateInput(CONNECTION, false)) }
    fixture.grant(Permission.PermissionType.WORKSPACE_RUNNER)
    assertDoesNotThrow { rule.authorize(embedded(WORKSPACE), PublicJobCreateInput(CONNECTION, false)) }
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(embedded(WORKSPACE), PublicJobCreateInput(CONNECTION, true)) }
  }

  @Test
  fun `embedded destination reads and deletes keep issuer grants inside the token workspace`() {
    val fixture = Fixture()
    fixture.grant(Permission.PermissionType.WORKSPACE_EDITOR)
    every { fixture.data.actorConnectionIds(DESTINATION, WORKSPACE, false) } returns emptyList()
    val caller = embedded(WORKSPACE)

    assertDoesNotThrow { PublicDestinationReadRule(fixture.data, fixture.permissions).authorize(caller, PublicResourceInput(DESTINATION)) }
    assertDoesNotThrow { PublicDestinationDeleteRule(fixture.data, fixture.permissions).authorize(caller, PublicResourceInput(DESTINATION)) }
    assertThrows(ForbiddenProblem::class.java) {
      PublicDestinationReadRule(fixture.data, fixture.permissions).authorize(embedded(OTHER_WORKSPACE), PublicResourceInput(DESTINATION))
    }
  }

  private class Fixture {
    val data = mockk<AuthorizationDataService>().also { it.answerConnectionOwnersFromActorStubs() }
    val grants = mockk<PermissionService>()
    val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)

    init {
      every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns OWNER
      every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns OWNER
      every { data.replicationJobConnectionId(1L) } returns CONNECTION
      every { data.workspaceAndOrganization(WORKSPACE) } returns OWNER
    }

    fun grant(type: Permission.PermissionType) {
      every { grants.getPermissionsByAuthUserId("caller") } returns
        listOf(Permission().withWorkspaceId(WORKSPACE).withPermissionType(type))
    }
  }

  companion object {
    private val WORKSPACE = UUID(0, 1)
    private val OTHER_WORKSPACE = UUID(0, 2)
    private val ORGANIZATION = UUID(0, 3)
    private val SOURCE = UUID(0, 4)
    private val DESTINATION = UUID(0, 5)
    private val CONNECTION = UUID(0, 6)
    private val DEFINITION = UUID(0, 7)
    private val OWNER = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)

    private fun embedded(workspace: UUID) =
      Authentication.build(
        "caller",
        listOf(AuthRoleConstants.EMBEDDED_END_USER),
        mapOf(TokenType.EMBEDDED_V1.toClaim(), TokenScopeClaim.CLAIM_ID to TokenScopeClaim(workspace.toString())),
      )
  }
}
