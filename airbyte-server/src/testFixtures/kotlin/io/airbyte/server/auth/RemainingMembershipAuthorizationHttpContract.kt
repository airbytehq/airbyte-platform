/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.PermissionCheckRead
import io.airbyte.api.server.generated.models.PermissionRead
import io.airbyte.api.server.generated.models.PermissionReadList
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.UserInvitationAdminRead
import io.airbyte.api.server.generated.models.UserInvitationCreateResponse
import io.airbyte.api.server.generated.models.UserInvitationRead
import io.airbyte.api.server.generated.models.UserInvitationStatus
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.config.Permission
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.AuthorizationUserIdentity
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.airbyte.domain.services.auth.SavedAuthorizationInvitation
import io.airbyte.domain.services.auth.SavedAuthorizationPermission
import io.airbyte.server.apis.controllers.GroupApiController
import io.airbyte.server.apis.controllers.GroupPermissionApiController
import io.airbyte.server.apis.controllers.PermissionApiController
import io.airbyte.server.apis.controllers.UserInvitationApiController
import io.airbyte.server.handlers.UserInvitationHandler
import io.airbyte.server.helpers.GroupPermissionValidator
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.airbyte.server.helpers.UserInvitationAuthorizationHelper
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

abstract class RemainingMembershipAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `membership permission and invitation reads reject workload credentials before business despite administrator labels`() {
    val paths =
      setOf("/api/v1/groups/update", "/api/v1/group_permissions/list", "/api/v1/permissions/get", "/api/v1/user_invitations/by_code/fixture")
    assertAll(scenarios().filter { it.path in paths }.map { scenario -> Executable { exercise(scenario, 403, token = "workload") } })
  }

  @Test
  fun `membership saved relationships and invitation recipient checks reject before business`() {
    assertAll(
      Executable { exercise(scenarios().single { it.path == "/api/v1/groups/add_member" }, 404, foreignMember = true, checkDeclaration = false) },
      Executable {
        exercise(
          scenarios().single {
            it.path == "/api/v1/group_permissions/create"
          },
          404,
          foreignWorkspace = true,
          grant = Permission.PermissionType.INSTANCE_ADMIN,
          checkDeclaration = false,
        )
      },
      Executable {
        exercise(scenarios().single { it.path == "/api/v1/group_permissions/delete" }, 404, foreignPermissionGroup = true, checkDeclaration = false)
      },
      Executable {
        exercise(
          scenarios().single {
            it.path == "/api/v1/permissions/get"
          },
          403,
          inheritedPermission = true,
          grant = Permission.PermissionType.INSTANCE_ADMIN,
          checkDeclaration = false,
        )
      },
      Executable { exercise(scenarios().single { it.method == "GET" }, 403, foreignRecipient = true, checkDeclaration = false) },
      Executable { exercise(scenarios().single { it.method == "GET" }, 403, recipientCase = true, checkDeclaration = false) },
    )
  }

  @Test
  fun `all twenty five membership operations execute independent policies and normal or explicit unsupported results`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, scenario.status) } })
  }

  @Test
  fun `all twenty five membership operations reject before group permissions email credentials or audit identity`() {
    assertAll(
      scenarios().map { scenario ->
        Executable {
          exercise(scenario, 403, granted = false, foreignRecipient = scenario.recipient, token = if (scenario.stub) "workload" else "user")
        }
      },
    )
  }

  @Test
  fun `recipient case permission SELF and cancellation selector precedence preserve actual branches`() {
    val get = scenarios().single { it.method == "GET" }
    val accept = scenarios().single { it.path.endsWith("/accept") }
    exercise(accept, 200, recipientCase = true, granted = false)
    exercise(get, 200, granted = false)
    exercise(accept, 200, granted = false)
    val cancellation = scenarios().single { it.path.endsWith("/cancel") }
    exercise(cancellation.copy(body = """{"id":"$INVITATION","inviteCode":"ignored-other-code"}"""), 200)
    exercise(cancellation.copy(body = """{"inviteCode":"fixture"}"""), 200)
    val list = scenarios().single { it.path == "/api/v1/permissions/list_by_user" }
    exercise(list, 200, granted = false, self = true)
    exercise(list, 403, grant = Permission.PermissionType.WORKSPACE_READER)
    val leave = scenarios().single { it.path.endsWith("/delete_user_from_workspace") }
    exercise(leave, 200, granted = false, self = true)
    exercise(scenarios().single { it.path.endsWith("/remove_member") }, 204, missingBeneficiary = true)
    for (scenario in scenarios()) exercise(scenario, 403, token = "workload", grant = Permission.PermissionType.INSTANCE_ADMIN)
  }

  @Test
  fun `required membership lookup failures and nulls stop business while diagnostics keep empty target results`() {
    val read = scenarios().single { it.path == "/api/v1/permissions/get" }
    exercise(read, 404, missing = true)
    exercise(read, 500, lookupError = true)
    exercise(read, 200, userlessPermission = true)
    exercise(scenarios().single { it.path == "/api/v1/permissions/delete" }, 404, userlessPermission = true)
    exercise(scenarios().single { it.path == "/api/v1/permissions/update" }, 500, userlessPermission = true)
    exercise(scenarios().single { it.path == "/api/v1/permissions/update" }.copy(body = """{"permissionId":"$PERMISSION"}"""), 500)
    val cancel = scenarios().single { it.path.endsWith("/cancel") }
    exercise(cancel, 403, missing = true)
    exercise(cancel, 403, lookupError = true)
    exercise(cancel.copy(body = "{}"), 400)
    exercise(read.copy(body = "null"), 400)
    val diagnostic = scenarios().single { it.path.endsWith("/check_multiple_workspaces") }
    exercise(diagnostic.copy(body = """{"userId":"$OTHER_USER","permissionType":"workspace_reader","workspaceIds":[]}"""), 200)
    exercise(
      diagnostic.copy(body = """{"userId":"$OTHER_USER","permissionType":"workspace_reader","workspaceIds":["$WORKSPACE","$WORKSPACE"]}"""),
      200,
    )
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, scenario.status, security = false, granted = false) } })
  }

  @Test
  fun `group derived direct permission errors preserve their operation specific messages`() {
    for ((path, verb) in listOf("get" to "read", "update" to "updated", "delete" to "deleted")) {
      exercise(
        scenarios().single { it.path == "/api/v1/permissions/$path" },
        403,
        inheritedPermission = true,
        grant = Permission.PermissionType.INSTANCE_ADMIN,
        expectedMessage = "Permission is inherited from a group and cannot be $verb as a user permission.",
      )
    }
  }

  @Test
  fun `membership concrete caller kinds and saved workspace scopes remain distinct`() {
    for (scenario in scenarios().filter {
      it.rule.startsWith("Group") ||
        it.rule in setOf("InvitationReadRule", "InvitationAcceptRule", "InvitationScopeAdminRule", "InvitationCancelRule")
    }) {
      exercise(scenario, scenario.status, token = "application-owner")
      for (token in listOf(
        "internal",
        "dataplane",
        "service",
        "embedded",
      )) {
        exercise(scenario, 403, token = token, grant = Permission.PermissionType.INSTANCE_ADMIN)
      }
    }
    for (path in listOf("/api/v1/groups/create", "/api/v1/groups/list", "/api/v1/permissions/get", "/api/v1/user_invitations/list_pending")) {
      val scenario = scenarios().single { it.path == path }
      exercise(scenario, 200, token = "internal", granted = false)
      exercise(scenario, 200, token = "service")
    }
    for (path in listOf(
      "/api/v1/permissions/get",
      "/api/v1/permissions/create",
      "/api/v1/user_invitations/create",
      "/api/v1/user_invitations/list_pending",
    )) {
      val scenario = scenarios().single { it.path == path }
      exercise(scenario, 200, token = "scoped:$WORKSPACE")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}", grant = Permission.PermissionType.INSTANCE_ADMIN)
    }
    val cancel = scenarios().single { it.path.endsWith("/cancel") }
    exercise(cancel, 403, token = "scoped:$WORKSPACE")
    val decline = scenarios().single { it.stub }
    exercise(decline, 500, token = "internal", granted = false)
  }

  @Test
  fun `membership administration rejects actual readers rather than treating any saved access as administration`() {
    for (scenario in scenarios().filter {
      it.grant in
        setOf(Permission.PermissionType.ORGANIZATION_ADMIN, Permission.PermissionType.WORKSPACE_ADMIN, Permission.PermissionType.INSTANCE_ADMIN)
    }) {
      exercise(scenario, 403, grant = Permission.PermissionType.ORGANIZATION_READER)
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    granted: Boolean = true,
    grant: Permission.PermissionType = scenario.grant,
    token: String = "user",
    security: Boolean = true,
    self: Boolean = false,
    foreignMember: Boolean = false,
    foreignWorkspace: Boolean = false,
    foreignPermissionGroup: Boolean = false,
    inheritedPermission: Boolean = false,
    foreignRecipient: Boolean = false,
    recipientCase: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    checkDeclaration: Boolean = true,
    missingBeneficiary: Boolean = false,
    userlessPermission: Boolean = false,
    expectedMessage: String = "",
  ) {
    val data = mockk<AuthorizationDataService>()
    val membership = mockk<MembershipAuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, controllers(), scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(), security).run(
      configure = { context, business ->
        configureBusiness(business, foreignPermissionGroup, userlessPermission)
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.groupOrganization(GROUP) } returns ORGANIZATION
        every { data.workspaceAndOrganization(WORKSPACE) } returns
          WorkspaceAndOrganization(WORKSPACE, if (foreignWorkspace) UUID(0, 99) else ORGANIZATION)
        every { data.userAuthSubjects(USER) } returns setOf(if (self) "caller" else "other-subject")
        every { data.requireUser(USER) } just Runs
        if (missingBeneficiary) every { data.requireUser(USER) } throws AuthorizationResourceNotFoundException()
        every { membership.isDirectOrganizationMember(USER, ORGANIZATION) } returns !foreignMember
        every { membership.permissionGroup(PERMISSION) } returns Optional.of(if (foreignPermissionGroup) UUID(0, 99) else GROUP)
        every { membership.permission(PERMISSION) } returns
          SavedAuthorizationPermission(
            AuthorizationTarget.Workspace(WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)),
            if (userlessPermission) Optional.empty() else Optional.of(USER),
            if (inheritedPermission) Optional.of(GROUP) else Optional.empty(),
            Permission.PermissionType.WORKSPACE_READER,
          )
        val email =
          when {
            foreignRecipient -> "other@example.com"
            recipientCase -> "FIXTURE@example.com"
            else -> "fixture@example.com"
          }
        val invitation =
          SavedAuthorizationInvitation(
            AuthorizationTarget.Workspace(WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)),
            email,
            Permission.PermissionType.WORKSPACE_READER,
          )
        every { membership.invitationById(INVITATION) } returns invitation
        every { membership.invitationByCode("fixture") } returns invitation
        every { membership.currentIdentity("caller") } returns AuthorizationUserIdentity(USER, Optional.of("fixture@example.com"))
        every { membership.defaultIdentity() } returns AuthorizationUserIdentity(USER, Optional.of("fixture@example.com"))
        if (missing || lookupError) {
          val failure = if (missing) AuthorizationResourceNotFoundException() else java.io.IOException("membership metadata unavailable")
          every { membership.permission(PERMISSION) } throws failure
          every { membership.invitationById(INVITATION) } throws failure
        }
        val permissions =
          if (!granted) {
            emptyList()
          } else {
            listOf(
              Permission().withPermissionType(grant).also {
                if (grant.name.startsWith("ORGANIZATION")) it.organizationId = ORGANIZATION
                if (grant.name.startsWith("WORKSPACE")) it.workspaceId = WORKSPACE
              },
            )
          }
        every { grants.getPermissionsByAuthUserId("caller") } returns permissions
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns permissions
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(MembershipAuthorizationDataService::class.java, membership)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (checkDeclaration) {
          RemainingOperationHttpSupport.requireRule(
            context,
            scenario.method,
            scenario.template,
            "io.airbyte.server.auth.${scenario.rule}",
          )
        }
        val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expectedMessage.isNotEmpty()) assertTrue(response.body().contains(expectedMessage), response.body())
        if (expected != scenario.status || expected == 403 || expected == 404 ||
          expected == 400
        ) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
        if (expected == 200 && !scenario.empty) assertTrue(response.body().isNotBlank(), scenario.path)
        if (scenario.stub) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
          verify(exactly = 0) { membership.invitationByCode(any()) }
          verify(exactly = 0) { membership.currentIdentity(any()) }
        }
        if (scenario.path.endsWith("/cancel") && scenario.body.contains("\"id\"")) verify(exactly = 0) { membership.invitationByCode(any()) }
        if (!security) verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val grant: Permission.PermissionType,
    val status: Int = 200,
    val empty: Boolean = false,
    val recipient: Boolean = false,
    val stub: Boolean = false,
    val method: String = "POST",
    val template: String = path,
  )

  private fun scenarios(): List<Scenario> {
    val group = """{"groupId":"$GROUP"}"""
    val member = """{"groupId":"$GROUP","userId":"$USER"}"""
    val permission = """{"permissionId":"$PERMISSION"}"""
    val scope = """{"scopeType":"workspace","scopeId":"$WORKSPACE"}"""
    val admin = Permission.PermissionType.ORGANIZATION_ADMIN
    return listOf(
      Scenario("/api/v1/groups/create", """{"organizationId":"$ORGANIZATION","name":"fixture"}""", "OrganizationAdminRule", admin),
      Scenario("/api/v1/groups/list", """{"organizationId":"$ORGANIZATION"}""", "OrganizationAdminRule", admin),
      Scenario("/api/v1/groups/get", group, "GroupSavedAdminRule", admin),
      Scenario("/api/v1/groups/update", """{"groupId":"$GROUP","name":"fixture"}""", "GroupSavedAdminRule", admin),
      Scenario("/api/v1/groups/delete", group, "GroupSavedAdminRule", admin, status = 204),
      Scenario("/api/v1/groups/list_members", group, "GroupSavedAdminRule", admin),
      Scenario("/api/v1/groups/add_member", member, "GroupMemberAddRule", admin),
      Scenario("/api/v1/groups/remove_member", member, "GroupMemberRemoveRule", admin, status = 204),
      Scenario("/api/v1/group_permissions/list", group, "GroupSavedAdminRule", admin),
      Scenario(
        "/api/v1/group_permissions/create",
        """{"groupId":"$GROUP","workspaceId":"$WORKSPACE","permissionType":"workspace_reader"}""",
        "GroupPermissionCreateRule",
        admin,
      ),
      Scenario(
        "/api/v1/group_permissions/delete",
        """{"groupId":"$GROUP","permissionId":"$PERMISSION"}""",
        "GroupPermissionDeleteRule",
        admin,
        status = 204,
      ),
      Scenario(
        "/api/v1/permissions/create",
        """{"userId":"$USER","workspaceId":"$WORKSPACE","permissionType":"workspace_reader"}""",
        "PermissionCreateRule",
        Permission.PermissionType.WORKSPACE_ADMIN,
      ),
      Scenario("/api/v1/permissions/get", permission, "PermissionReadRule", Permission.PermissionType.WORKSPACE_READER),
      Scenario(
        "/api/v1/permissions/update",
        """{"permissionId":"$PERMISSION","permissionType":"workspace_reader"}""",
        "PermissionUpdateRule",
        Permission.PermissionType.WORKSPACE_ADMIN,
        empty = true,
      ),
      Scenario("/api/v1/permissions/delete", permission, "PermissionDeleteRule", Permission.PermissionType.WORKSPACE_ADMIN, empty = true),
      Scenario(
        "/api/v1/permissions/delete_user_from_workspace",
        """{"userId":"$USER","workspaceId":"$WORKSPACE"}""",
        "PermissionLeaveWorkspaceRule",
        Permission.PermissionType.WORKSPACE_ADMIN,
        empty = true,
      ),
      Scenario("/api/v1/permissions/list_by_user", """{"userId":"$USER"}""", "PermissionUserListRule", Permission.PermissionType.INSTANCE_ADMIN),
      Scenario(
        "/api/v1/permissions/check",
        """{"userId":"$OTHER_USER","workspaceId":"$WORKSPACE","permissionType":"workspace_reader"}""",
        "PermissionDiagnosticRule",
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/permissions/check_multiple_workspaces",
        """{"userId":"$OTHER_USER","workspaceIds":[],"permissionType":"workspace_reader"}""",
        "PermissionDiagnosticRule",
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/user_invitations/create",
        """{"scopeType":"workspace","scopeId":"$WORKSPACE","permissionType":"workspace_reader","invitedEmail":"fixture@example.com"}""",
        "InvitationScopeAdminRule",
        Permission.PermissionType.WORKSPACE_ADMIN,
      ),
      Scenario("/api/v1/user_invitations/list_pending", scope, "InvitationScopeReadRule", Permission.PermissionType.WORKSPACE_READER),
      Scenario(
        "/api/v1/user_invitations/by_code/fixture",
        "",
        "InvitationReadRule",
        Permission.PermissionType.WORKSPACE_READER,
        recipient = true,
        method = "GET",
        template = "/api/v1/user_invitations/by_code/{inviteCode}",
      ),
      Scenario(
        "/api/v1/user_invitations/accept",
        """{"inviteCode":"fixture"}""",
        "InvitationAcceptRule",
        Permission.PermissionType.WORKSPACE_READER,
        recipient = true,
      ),
      Scenario(
        "/api/v1/user_invitations/decline",
        """{"inviteCode":"ignored"}""",
        "AuthenticatedMetadataRule",
        Permission.PermissionType.WORKSPACE_READER,
        status = 500,
        stub = true,
      ),
      Scenario("/api/v1/user_invitations/cancel", """{"id":"$INVITATION"}""", "InvitationCancelRule", Permission.PermissionType.WORKSPACE_ADMIN),
    )
  }

  private fun configureBusiness(
    business: List<Any>,
    foreignPermissionGroup: Boolean = false,
    userlessPermission: Boolean = false,
  ) {
    val time = OffsetDateTime.parse("2026-10-01T00:00:00Z")
    val group = Group(GroupId(GROUP), "fixture", null, OrganizationId(ORGANIZATION), 0, time, time)
    business.forEach { instance ->
      when (instance) {
        is GroupService -> {
          every { instance.getGroup(GroupId(GROUP)) } returns group
          every { instance.updateGroup(match { it.groupId == GroupId(GROUP) }) } returns group
          every { instance.createGroup(match { it.organizationId == OrganizationId(ORGANIZATION) && it.name == "fixture" }) } returns group
          every { instance.getGroupsForOrganization(OrganizationId(ORGANIZATION), null) } returns emptyList()
          every { instance.getGroupMembers(GroupId(GROUP)) } returns emptyList()
          every { instance.deleteGroup(GroupId(GROUP), OrganizationId(ORGANIZATION)) } just Runs
          every { instance.addGroupMember(GroupId(GROUP), UserId(USER), OrganizationId(ORGANIZATION)) } returns
            GroupMember(UUID(0, 10), GROUP, USER, "fixture@example.com", "fixture", time)
          every { instance.removeGroupMember(GroupId(GROUP), UserId(USER), OrganizationId(ORGANIZATION)) } just Runs
        }
        is RoleResolver -> {
          val request = mockk<RoleResolver.Request>()
          every { instance.newRequest() } returns request
          every { request.withCurrentUser() } returns request
          every { request.withRef(any(), ORGANIZATION.toString()) } returns request
          every { request.requireRole(any()) } just Runs
        }
        is GroupsEntitlementHelper -> every { instance.ensureEntitled(OrganizationId(ORGANIZATION)) } just Runs
        is PermissionService -> {
          every { instance.getPermissionsByGroupId(GROUP) } returns emptyList()
          val permission =
            Permission()
              .withPermissionId(
                PERMISSION,
              ).withWorkspaceId(
                WORKSPACE,
              ).withGroupId(if (foreignPermissionGroup) UUID(0, 99) else GROUP)
              .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          every { instance.getPermission(PERMISSION) } returns permission
          every { instance.createGroupPermission(match { it.groupId == GROUP && it.workspaceId == WORKSPACE }) } returns permission
          every { instance.deleteGroupPermission(PERMISSION) } just Runs
        }
        is GroupPermissionValidator -> {
          every { instance.validateScope(match { it.groupId == GroupId(GROUP) }, any(), WORKSPACE, null) } just Runs
          every { instance.isDuplicate(GROUP, any(), WORKSPACE, null) } returns false
        }
        is PermissionHandler -> {
          every { instance.getPermissionRead(match { it.permissionId == PERMISSION }) } returns
            PermissionRead(PERMISSION, PermissionType.WORKSPACE_READER, userId = if (userlessPermission) null else USER, workspaceId = WORKSPACE)
          every { instance.createPermission(match { it.userId == USER && it.workspaceId == WORKSPACE }) } returns
            Permission()
              .withPermissionId(
                PERMISSION,
              ).withUserId(USER)
              .withWorkspaceId(WORKSPACE)
              .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          every { instance.updatePermission(match { it.permissionId == PERMISSION }) } just Runs
          every { instance.deletePermission(match { it.permissionId == PERMISSION }) } just Runs
          every { instance.deleteUserFromWorkspace(match { it.userId == USER && it.workspaceId == WORKSPACE }) } just Runs
          every { instance.effectivePermissionReadListForUser(USER) } returns PermissionReadList(emptyList())
          every { instance.checkPermissions(match { it.userId == OTHER_USER }) } returns PermissionCheckRead(PermissionCheckRead.Status.FAILED)
          every { instance.permissionsCheckMultipleWorkspaces(match { it.userId == OTHER_USER }) } returns
            PermissionCheckRead(PermissionCheckRead.Status.SUCCEEDED)
        }
        is CurrentUserService ->
          every { instance.getCurrentUser() } returns
            AuthenticatedUser().withUserId(USER).withAuthUserId("caller").withEmail("fixture@example.com")
        is UserInvitationHandler -> {
          val invitation =
            UserInvitationRead(
              INVITATION,
              "fixture",
              USER,
              "fixture@example.com",
              WORKSPACE,
              ScopeType.WORKSPACE,
              PermissionType.WORKSPACE_READER,
              UserInvitationStatus.PENDING,
              0,
              0,
            )
          every { instance.getByInviteCode("fixture", match { it.userId == USER }) } returns invitation
          every { instance.accept(match { it.inviteCode == "fixture" }, match { it.userId == USER }) } returns
            invitation.copy(status = UserInvitationStatus.ACCEPTED)
          every { instance.getPendingInvitations(match { it.scopeId == WORKSPACE }) } returns emptyList()
          every { instance.createInvitationOrPermission(match { it.scopeId == WORKSPACE }, match { it.userId == USER }) } returns
            UserInvitationCreateResponse(false)
          every { instance.cancel(any()) } returns
            UserInvitationAdminRead(
              INVITATION,
              USER,
              "fixture@example.com",
              WORKSPACE,
              ScopeType.WORKSPACE,
              PermissionType.WORKSPACE_READER,
              UserInvitationStatus.CANCELLED,
              0,
              0,
            )
        }
        is UserInvitationAuthorizationHelper -> {
          every { instance.authorizeInvitationAdmin(INVITATION, USER) } just Runs
          every { instance.authorizeInvitationAdmin("fixture", USER) } just Runs
        }
      }
    }
  }

  private fun controllers() =
    listOf(
      GroupApiController::class.java,
      GroupPermissionApiController::class.java,
      PermissionApiController::class.java,
      UserInvitationApiController::class.java,
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val GROUP: UUID = UUID(0, 3)
    val PERMISSION: UUID = UUID(0, 4)
    val USER: UUID = UUID(0, 8)
    val INVITATION: UUID = UUID(0, 9)
    val OTHER_USER: UUID = UUID(0, 99)
  }
}
