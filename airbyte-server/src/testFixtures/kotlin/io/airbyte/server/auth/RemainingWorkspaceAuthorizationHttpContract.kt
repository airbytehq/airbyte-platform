/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.OrganizationInfoRead
import io.airbyte.api.server.generated.models.PermissionCheckRead
import io.airbyte.api.server.generated.models.WorkspaceRead
import io.airbyte.api.server.generated.models.WorkspaceReadList
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.WorkspaceApiController
import io.micronaut.security.utils.SecurityService
import io.mockk.Called
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
import java.util.Optional
import java.util.UUID

abstract class RemainingWorkspaceAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `workspace reads reject workload credentials before reading configuration despite administrator labels`() {
    val controller = if (cloud) Class.forName("io.airbyte.server.wrapped.apis.WorkspaceApiWrappedController") else WorkspaceApiController::class.java
    RemainingOperationHttpSupport(cloud, listOf(controller), setOf(WorkspaceGetRule::class.java.name)).run(
      configure = { context, business ->
        business.filterIsInstance<WorkspacesHandler>().forEach { handler ->
          every { handler.getWorkspace(match { it.workspaceId == WORKSPACE }) } returns workspace()
        }
        val data = mockk<AuthorizationDataService>()
        every { data.savedWorkspaceAndOrganization(WORKSPACE, false) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(mockk<PermissionService>(), RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
      },
      check = { _, server, business ->
        val response = RemainingOperationHttpSupport.request(server, "POST", "/api/v1/workspaces/get", """{"workspaceId":"$WORKSPACE"}""", "workload")
        assertEquals(403, response.statusCode(), response.body())
        RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  @Test
  fun `workspace multi resource selectors reject foreign recreation dbt configuration and capacity organization before business`() {
    assertAll(
      Executable { exercise(scenarios().single { it.path.endsWith("/create_if_not_exist") }, 403, foreignExisting = true, checkDeclaration = false) },
      Executable {
        exercise(
          scenarios().single { it.path.endsWith("/get_available_dbt_jobs") },
          if (cloud) 500 else 501,
          foreignDbt = true,
          checkDeclaration = false,
        )
      },
      Executable {
        exercise(
          scenarios()
            .single {
              it.path.endsWith("/get_data_worker_availability")
            }.copy(body = """{"workspaceId":"$WORKSPACE","organizationId":"$OTHER_ORGANIZATION"}"""),
          if (cloud) 400 else 501,
          checkDeclaration = false,
        )
      },
      Executable {
        exercise(
          scenarios()
            .single {
              it.path.endsWith("/list_paginated")
            }.copy(
              body = """{"workspaceIds":["$WORKSPACE","$OTHER_WORKSPACE"],"pagination":{"pageSize":20,"rowOffset":0}}""",
            ),
          403,
          checkDeclaration = false,
        )
      },
    )
  }

  @Test
  fun `OSS workspace stubs preserve action checks without Cloud selectors or no auth metadata reads`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.cloudOnly && !it.template.endsWith("/skills/docs") }.map { scenario ->
        Executable {
          exercise(scenario, 501, foreignDbt = true, checkDeclaration = false)
          exercise(scenario, 403, granted = false, checkDeclaration = false)
          exercise(scenario, 403, token = "workload", checkDeclaration = false)
          exercise(scenario, 403, token = "scoped:$OTHER_WORKSPACE", checkDeclaration = false)
          exercise(scenario, 501, missing = true, security = false, granted = false, checkDeclaration = false, unusedOwner = true)
          exercise(scenario, 501, lookupFailure = true, security = false, granted = false, checkDeclaration = false, unusedOwner = true)
        }
      },
    )
  }

  @Test
  fun `OSS workspace stubs keep instance admin and nullable no auth requests independent of tenant metadata`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.cloudOnly && !it.template.endsWith("/skills/docs") }.map { scenario ->
        Executable {
          exercise(scenario, 501, grant = Permission.PermissionType.INSTANCE_ADMIN, missing = true, checkDeclaration = false, unusedOwner = true)
        }
      } +
        scenarios().filter { it.path.endsWith("/get_usage") || it.path.endsWith("/get_data_worker_availability") }.map { scenario ->
          Executable {
            exercise(scenario.copy(body = "null"), 501, security = false, granted = false, checkDeclaration = false, unusedOwner = true)
          }
        },
    )
  }

  @Test
  fun `all nineteen workspace operations execute independent expected actions and normal responses`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, status(scenario)) } })
  }

  @Test
  fun `canonical skill documentation actor selectors require the saved kind and workspace before the builder`() {
    val skill = scenarios().single { it.template.endsWith("/skills/docs") }
    assertAll(
      listOf("source", "destination").map { kind ->
        Executable {
          exercise(
            skill.copy(path = "/api/v1/workspaces/$WORKSPACE/skills/docs?id=connector-$kind:$SKILL_ACTOR&section=details"),
            if (cloud) 404 else 501,
            foreignSkill = true,
            checkDeclaration = false,
          )
        }
      },
    )
  }

  @Test
  fun `workspace operations preserve grant checks and OSS unsupported responses before business`() {
    assertAll(
      scenarios().map { scenario ->
        Executable {
          exercise(
            scenario,
            if (!cloud &&
              scenario.rule == OssUnsupportedOperationRule::class.java
            ) {
              501
            } else {
              403
            },
            granted = false,
            token =
              if (scenario.rule ==
                WorkspaceFilteredOrganizationRule::class.java
              ) {
                "internal"
              } else {
                "user"
              },
          )
        }
      },
    )
  }

  @Test
  fun `workspace scopes deleted rows filtered lists and worker roles preserve concrete alternatives`() {
    val get = scenarios().single { it.path == "/api/v1/workspaces/get" }
    exercise(get.copy(body = """{"workspaceId":"$WORKSPACE","includeTombstone":true}"""), 200)
    exercise(get, 200, token = "scoped:$WORKSPACE")
    exercise(get, 403, token = "scoped:$OTHER_WORKSPACE", grant = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(get, 200, token = "embedded", granted = false)
    exercise(get, 200, token = "dataplane", grant = Permission.PermissionType.DATAPLANE)
    val filtered = scenarios().single { it.rule == WorkspaceFilteredOrganizationRule::class.java }
    exercise(filtered, 200, granted = false)
    exercise(filtered, 403, token = "scoped:$WORKSPACE")
    val byUser = scenarios().single { it.rule == WorkspaceUserListRule::class.java }
    exercise(byUser, 200, granted = false, self = true)
    exercise(byUser, 403, grant = Permission.PermissionType.WORKSPACE_READER)
    for (scenario in scenarios()) {
      exercise(
        scenario,
        if (!cloud &&
          scenario.rule == OssUnsupportedOperationRule::class.java
        ) {
          501
        } else {
          403
        },
        token = "workload",
        grant = Permission.PermissionType.INSTANCE_ADMIN,
      )
    }
    val list = scenarios().single { it.rule == WorkspaceListRule::class.java }
    exercise(
      list.copy(body = """{"workspaceIds":["$WORKSPACE","$WORKSPACE"],"includeDeleted":true,"pagination":{"pageSize":20,"rowOffset":0}}"""),
      200,
    )
    exercise(
      list.copy(body = """{"workspaceIds":[],"pagination":{"pageSize":20,"rowOffset":0}}"""),
      200,
      grant = Permission.PermissionType.INSTANCE_ADMIN,
    )
    exercise(list.copy(body = """{"workspaceIds":[],"pagination":{"pageSize":20,"rowOffset":0}}"""), 403)
  }

  @Test
  fun `required workspace metadata failures never fall back and no auth preserves normal contracts`() {
    val get = scenarios().single { it.path == "/api/v1/workspaces/get" }
    exercise(get, 404, missing = true)
    exercise(get, 500, lookupFailure = true)
    exercise(get.copy(body = "{}"), 400)
    exercise(scenarios().single { it.rule == WorkspaceCreateRule::class.java && it.path.endsWith("/create_if_not_exist") }, 500, lookupFailure = true)
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, status(scenario), security = false, granted = false) } })
  }

  @Test
  fun `OSS skill docs stub preserves unsupported response without saved workspace or tenant grants`() {
    if (cloud) return
    val skill = scenarios().single { it.template.endsWith("/skills/docs") }
    assertAll(
      { exercise(skill, 501, granted = false, checkDeclaration = false, unusedTenant = true) },
      { exercise(skill, 401, token = "anonymous", checkDeclaration = false, unusedTenant = true) },
      { exercise(skill, 501, token = "workload", checkDeclaration = false, unusedTenant = true) },
      { exercise(skill, 501, token = "embedded", checkDeclaration = false, unusedTenant = true) },
      { exercise(skill, 403, token = "scoped:$WORKSPACE", checkDeclaration = false, unusedTenant = true) },
      { exercise(skill, 501, granted = false, security = false, missing = true, checkDeclaration = false, unusedTenant = true) },
    )
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    granted: Boolean = true,
    grant: Permission.PermissionType = scenario.grant,
    token: String = "user",
    security: Boolean = true,
    self: Boolean = false,
    foreignExisting: Boolean = false,
    foreignDbt: Boolean = false,
    missing: Boolean = false,
    lookupFailure: Boolean = false,
    checkDeclaration: Boolean = true,
    foreignSkill: Boolean = false,
    unusedTenant: Boolean = false,
    unusedOwner: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val controller = if (cloud) Class.forName("io.airbyte.server.wrapped.apis.WorkspaceApiWrappedController") else WorkspaceApiController::class.java
    RemainingOperationHttpSupport(cloud, listOf(controller), rules(), security, earlierAllow = token != "anonymous").run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is WorkspacesHandler -> {
              every { instance.getWorkspace(match { it.workspaceId == WORKSPACE }) } returns workspace()
              every { instance.getWorkspaceBySlug(match { it.slug == "fixture" }) } returns workspace()
              every { instance.getWorkspaceOrganizationInfo(match { it.workspaceId == WORKSPACE }) } returns
                OrganizationInfoRead(ORGANIZATION, "fixture", false, false)
              every { instance.getWorkspaceByConnectionId(match { it.connectionId == CONNECTION }, any()) } returns workspace()
              every { instance.listWorkspacesPaginated(any()) } returns WorkspaceReadList(emptyList())
              every { instance.listWorkspacesInOrganizationForUser(USER, match { it.organizationId == ORGANIZATION }) } returns
                WorkspaceReadList(emptyList())
              every { instance.listWorkspacesByUser(match { it.userId == USER }) } returns WorkspaceReadList(emptyList())
              every { instance.createWorkspace(match { it.organizationId == ORGANIZATION && it.name == "fixture" }) } returns workspace()
              every { instance.createWorkspaceIfNotExist(match { it.id == WORKSPACE && it.organizationId == ORGANIZATION }) } returns workspace()
              every { instance.updateWorkspace(match { it.workspaceId == WORKSPACE }) } returns workspace()
              every { instance.updateWorkspaceName(match { it.workspaceId == WORKSPACE && it.name == "fixture" }) } returns workspace()
              every { instance.updateWorkspaceOrganization(match { it.workspaceId == WORKSPACE && it.organizationId == ORGANIZATION }) } returns
                workspace()
              every { instance.setFeedbackDone(match { it.workspaceId == WORKSPACE }) } just Runs
              every { instance.deleteWorkspace(match { it.workspaceId == WORKSPACE }) } just Runs
            }
            is PermissionHandler -> every { instance.checkPermissions(any()) } returns PermissionCheckRead(PermissionCheckRead.Status.SUCCEEDED)
            is CurrentUserService -> every { instance.getCurrentUser() } returns AuthenticatedUser().withUserId(USER).withAuthUserId("caller")
            is SecurityService -> every { instance.hasRole(AuthRoleConstants.ADMIN) } returns true
          }
          configureDependency(instance)
        }
        every { data.savedWorkspaceAndOrganization(any(), any()) } throws AuthorizationResourceNotFoundException()
        every { data.savedWorkspaceAndOrganization(WORKSPACE, any()) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.savedWorkspaceAndOrganization(OTHER_WORKSPACE, any()) } returns WorkspaceAndOrganization(OTHER_WORKSPACE, OTHER_ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceBySlug("fixture") } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.organization(OTHER_ORGANIZATION) } returns OTHER_ORGANIZATION
        every { data.savedConnectionActorIds(CONNECTION) } returns (UUID(0, 30) to UUID(0, 31))
        every { data.savedSourceWorkspaceAndOrganization(UUID(0, 30)) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.connectionSourceWorkspaceAndOrganization(CONNECTION, any()) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.savedSourceWorkspaceAndOrganization(SKILL_ACTOR) } returns
          if (foreignSkill) WorkspaceAndOrganization(OTHER_WORKSPACE, OTHER_ORGANIZATION) else WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.savedDestinationWorkspaceAndOrganization(SKILL_ACTOR) } returns
          if (foreignSkill) WorkspaceAndOrganization(OTHER_WORKSPACE, OTHER_ORGANIZATION) else WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.existingWorkspaceAndOrganization(WORKSPACE) } returns
          Optional.of(WorkspaceAndOrganization(WORKSPACE, if (foreignExisting) OTHER_ORGANIZATION else ORGANIZATION))
        every { data.userAuthSubjects(USER) } returns setOf(if (self) "caller" else "other-subject")
        every { data.requireWorkspaceDbtConfig(WORKSPACE, DBT) } just Runs
        if (foreignDbt) every { data.requireWorkspaceDbtConfig(WORKSPACE, DBT) } throws IllegalArgumentException("No such webhook config id")
        if (missing) every { data.savedWorkspaceAndOrganization(WORKSPACE, any()) } throws AuthorizationResourceNotFoundException()
        if (lookupFailure) {
          every { data.savedWorkspaceAndOrganization(WORKSPACE, any()) } throws java.io.IOException("workspace unavailable")
          every { data.existingWorkspaceAndOrganization(WORKSPACE) } throws java.io.IOException("workspace unavailable")
        }
        val permissions =
          if (!granted) {
            emptyList()
          } else {
            listOf(
              Permission().withPermissionType(grant).also {
                if (grant.name.startsWith("WORKSPACE")) it.workspaceId = WORKSPACE
                if (grant.name.startsWith("ORGANIZATION")) it.organizationId = ORGANIZATION
              },
            )
          }
        every { grants.getPermissionsByAuthUserId("caller") } returns permissions
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns permissions
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (checkDeclaration) RemainingOperationHttpSupport.requireRule(context, scenario.method, scenario.template, scenario.rule.name)
        val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected !in setOf(200, 204, 501)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200 && !scenario.empty) assertTrue(response.body().isNotBlank(), scenario.path)
        if (!cloud && scenario.rule == OssUnsupportedOperationRule::class.java) {
          verify(exactly = 0) { data.savedSourceWorkspaceAndOrganization(SKILL_ACTOR) }
          verify(exactly = 0) { data.savedDestinationWorkspaceAndOrganization(SKILL_ACTOR) }
        }
        if (unusedTenant || (!cloud && scenario.rule == OssUnsupportedOperationRule::class.java)) {
          verify { data wasNot Called }
          verify { grants wasNot Called }
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
        if (!cloud && scenario.cloudOnly) {
          verify(exactly = 0) { data.requireWorkspaceDbtConfig(any(), any()) }
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
        if (unusedOwner) verify { data wasNot Called }
        if (!security) verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
      },
    )
  }

  private fun status(scenario: Scenario) = if (!cloud && scenario.cloudOnly) 501 else scenario.status

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: Class<*>,
    val grant: Permission.PermissionType = Permission.PermissionType.WORKSPACE_READER,
    val status: Int = 200,
    val cloudOnly: Boolean = false,
    val empty: Boolean = false,
    val method: String = "POST",
    val template: String = path,
  )

  private fun scenarios(): List<Scenario> {
    val workspace = """{"workspaceId":"$WORKSPACE"}"""
    val connection = """{"connectionId":"$CONNECTION"}"""
    return listOf(
      Scenario("/api/v1/workspaces/get", workspace, WorkspaceGetRule::class.java),
      Scenario("/api/v1/workspaces/get_by_slug", """{"slug":"fixture"}""", WorkspaceReadRule::class.java),
      Scenario("/api/v1/workspaces/get_organization_info", workspace, WorkspaceReadRule::class.java),
      Scenario("/api/v1/workspaces/get_by_connection_id", connection, WorkspaceReadRule::class.java),
      Scenario("/api/v1/workspaces/get_by_connection_id_with_tombstone", connection, WorkspaceTombstoneConnectionRule::class.java),
      Scenario(
        "/api/v1/workspaces/get_usage",
        """{"workspaceId":"$WORKSPACE","timeWindow":"lastMonth"}""",
        if (cloud) WorkspaceReadRule::class.java else WorkspaceReadUnsupportedRule::class.java,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/workspaces/get_data_worker_availability",
        """{"workspaceId":"$WORKSPACE","organizationId":"$ORGANIZATION"}""",
        if (cloud) WorkspaceReadRule::class.java else WorkspaceReadUnsupportedRule::class.java,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/workspaces/list_paginated",
        """{"workspaceIds":["$WORKSPACE"],"pagination":{"pageSize":20,"rowOffset":0}}""",
        WorkspaceListRule::class.java,
      ),
      Scenario(
        "/api/v1/workspaces/list_by_organization_id",
        """{"organizationId":"$ORGANIZATION"}""",
        WorkspaceFilteredOrganizationRule::class.java,
        Permission.PermissionType.WORKSPACE_READER,
      ),
      Scenario(
        "/api/v1/workspaces/list_by_user_id",
        """{"userId":"$USER"}""",
        WorkspaceUserListRule::class.java,
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/workspaces/create",
        """{"organizationId":"$ORGANIZATION","name":"fixture"}""",
        WorkspaceCreateRule::class.java,
        Permission.PermissionType.ORGANIZATION_ADMIN,
      ),
      Scenario(
        "/api/v1/workspaces/create_if_not_exist",
        """{"id":"$WORKSPACE","organizationId":"$ORGANIZATION","name":"fixture"}""",
        WorkspaceCreateRule::class.java,
        Permission.PermissionType.ORGANIZATION_ADMIN,
      ),
      Scenario("/api/v1/workspaces/update", workspace, WorkspaceWriteRule::class.java, Permission.PermissionType.WORKSPACE_SOURCE_EDITOR),
      Scenario(
        "/api/v1/workspaces/update_name",
        """{"workspaceId":"$WORKSPACE","name":"fixture"}""",
        WorkspaceWriteRule::class.java,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ),
      Scenario(
        "/api/v1/workspaces/tag_feedback_status_as_done",
        workspace,
        WorkspaceWriteRule::class.java,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        empty = true,
      ),
      Scenario(
        "/api/v1/workspaces/update_organization",
        """{"workspaceId":"$WORKSPACE","organizationId":"$ORGANIZATION"}""",
        WorkspaceTransferRule::class.java,
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario("/api/v1/workspaces/delete", workspace, WorkspaceAdminRule::class.java, Permission.PermissionType.WORKSPACE_ADMIN, status = 204),
      Scenario(
        "/api/v1/workspaces/get_available_dbt_jobs",
        """{"workspaceId":"$WORKSPACE","dbtConfigId":"$DBT"}""",
        if (cloud) WorkspaceWriteRule::class.java else WorkspaceWriteUnsupportedRule::class.java,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/workspaces/$WORKSPACE/skills/docs?id=fixture",
        "",
        if (cloud) Class.forName("io.airbyte.server.wrapped.auth.WorkspaceSkillDocsActorRule") else OssUnsupportedOperationRule::class.java,
        cloudOnly = true,
        method = "GET",
        template = "/api/v1/workspaces/{workspaceId}/skills/docs",
      ),
    )
  }

  private fun rules() = scenarios().map { it.rule.name }.toSet()

  companion object {
    val WORKSPACE = UUID(0, 1)
    val ORGANIZATION = UUID(0, 2)
    val OTHER_ORGANIZATION = UUID(0, 22)
    val OTHER_WORKSPACE = UUID(0, 21)
    val USER = UUID(0, 8)
    val CONNECTION = UUID(0, 10)
    val DBT = UUID(0, 11)
    val SKILL_ACTOR = UUID.fromString("ad123456-7890-1234-abcd-1234567890ab")

    fun workspace() = WorkspaceRead(WORKSPACE, UUID(0, 3), "fixture", "fixture", false, ORGANIZATION)
  }
}
