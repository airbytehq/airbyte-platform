/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.DeclarativeManifestVersionRead
import io.airbyte.api.server.generated.models.DeclarativeManifestsReadList
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.DeclarativeSourceDefinitionsApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingDeclarativeAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `declarative manifest operations reject unsupported callers before manifest reads and writes`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `manifest access requires the exact custom source grant rather than ordinary catalog availability`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 404, usable = false, declaration = false) } })
  }

  @Test
  fun `three declarative operations preserve source editor policy even for listing and require saved scope and kind`() {
    for (scenario in scenarios()) {
      exercise(scenario, scenario.success)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR)
      exercise(scenario, scenario.success, grant = Permission.PermissionType.ORGANIZATION_EDITOR)
      exercise(scenario, scenario.success, token = "internal")
      exercise(scenario, 403, token = "embedded-scoped:$WORKSPACE", granted = false)
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, scenario.success, token = "scoped:$WORKSPACE")
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, scenario.success, granted = false, security = false)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
    granted: Boolean = true,
    usable: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    val workspace = mockk<WorkspaceService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(DeclarativeSourceDefinitionsApiController::class.java),
      setOf(
        "io.airbyte.server.auth.DeclarativeManifestRule",
        "io.airbyte.domain.services.auth.DeclarativeManifestAuthorizationService",
      ),
      security,
    ).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<DeclarativeSourceDefinitionsHandler>().single()
        every {
          handler.createDeclarativeSourceDefinitionManifest(
            match {
              it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION &&
                it.declarativeManifest.version == 1L
            },
          )
        } just Runs
        every {
          handler.updateDeclarativeManifestVersion(match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION && it.version == 1L })
        } just Runs
        every { handler.listManifestVersions(match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION }) } returns
          DeclarativeManifestsReadList(listOf(DeclarativeManifestVersionRead(1, true, "fixture")))
        every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.requireActorDefinitionType(DEFINITION, true) } just Runs
        every { workspace.workspaceCanUseCustomDefinition(DEFINITION, WORKSPACE) } returns usable
        if (missing) every { data.requireActorDefinitionType(DEFINITION, true) } throws AuthorizationResourceNotFoundException()
        if (lookupError) {
          every { workspace.workspaceCanUseCustomDefinition(DEFINITION, WORKSPACE) } throws
            java.io.IOException("manifest grant unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("WORKSPACE")) permission.workspaceId = WORKSPACE else permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(WorkspaceService::class.java, workspace)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.DeclarativeManifestRule")
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != scenario.success) RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val body: String,
    val success: Int,
  ) {
    val path = "/api/v1/declarative_source_definitions/$suffix"
  }

  private fun scenarios(): List<Scenario> {
    val target = "\"workspaceId\":\"$WORKSPACE\",\"sourceDefinitionId\":\"$DEFINITION\""
    return listOf(
      Scenario(
        "create_manifest",
        """{$target,"setAsActiveManifest":true,"declarativeManifest":{"description":"fixture","manifest":{},"spec":{},"version":1}}""",
        201,
      ),
      Scenario("update_active_manifest", """{$target,"version":1}""", 204),
      Scenario("list_manifests", """{$target}""", 200),
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
  }
}
