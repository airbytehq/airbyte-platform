/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ActorDefinitionCreateResponse
import io.airbyte.api.server.generated.models.ActorDefinitionUpdateResponse
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.DefinitionAccessService
import io.airbyte.server.apis.controllers.ActorDefinitionApiController
import io.airbyte.server.handlers.ActorDefinitionsHandler
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingActorDefinitionAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `generic definition mutations reject unsupported callers before workflows or custom definition checks`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `generic definition updates bind the actor kind and saved default version before workflow submission`() {
    assertAll(
      scenarios().filterNot { it.create }.flatMap { scenario ->
        listOf(
          Executable {
            exercise(
              scenario.copy(
                body =
                  scenario.body.replace(
                    "\"actorType\":\"${scenario.kind}\"",
                    "\"actorType\":\"${if (scenario.kind == "source") "destination" else "source"}\"",
                  ),
              ),
              404,
              declaration = false,
            )
          },
          Executable { exercise(scenario, 404, missingDefault = true, declaration = false) },
        )
      },
    )
  }

  @Test
  fun `generic definition update keeps the actual edition custom access predicate rather than trusting principal admin labels`() {
    if (!cloud) return
    for (scenario in scenarios().filterNot { it.create }) exercise(scenario, 422, customAccess = false, declaration = false)
  }

  @Test
  fun `generic Cloud update keeps missing current user authentication errors`() {
    if (!cloud) return
    for (scenario in scenarios().filterNot { it.create }) exercise(scenario, 401, identityError = true)
  }

  @Test
  fun `generic create and update declare exact policy and preserve supported workspace and embedded behavior`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 200, token = "application-owner")
      exercise(scenario, 200, token = "internal")
      exercise(scenario, 200, token = "scoped:$WORKSPACE")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, 200, granted = false, security = false)
      if (scenario.create) {
        exercise(scenario, 403, granted = false)
        exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
        exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR)
        exercise(scenario, 403, token = "embedded-scoped:$WORKSPACE")
        exercise(scenario.copy(body = scenario.body.dropLast(1) + ",\"scopeId\":\"${UUID(0, 99)}\",\"scopeType\":\"organization\"}"), 200)
      } else {
        exercise(scenario, 200, granted = false)
        exercise(scenario, 200, token = "embedded-scoped:$WORKSPACE")
        if (cloud) {
          exercise(scenario, 422, token = "embedded-scoped:$WORKSPACE", grant = Permission.PermissionType.INSTANCE_ADMIN, customAccess = false)
          exercise(scenario, 200, grant = Permission.PermissionType.INSTANCE_ADMIN, customAccess = false)
        }
      }
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.create) {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.WORKSPACE_READER
      },
    granted: Boolean = true,
    security: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    missingDefault: Boolean = false,
    customAccess: Boolean = true,
    identityError: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val access = mockk<DefinitionAccessService>()
    val controller =
      if (cloud) {
        Class.forName("io.airbyte.server.wrapped.apis.ActorDefinitionApiWrappedController")
      } else {
        ActorDefinitionApiController::class.java
      }
    RemainingOperationHttpSupport(cloud, listOf(controller), scenarios().map { it.rule }.toSet(), security).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<ActorDefinitionsHandler>().single()
        every {
          handler.createActorDefinition(match { it.workspaceId == WORKSPACE && it.scopeId == WORKSPACE && it.scopeType == ScopeType.WORKSPACE })
        } returns ActorDefinitionCreateResponse(REQUEST)
        every {
          handler.updateActorDefinition(
            match {
              it.workspaceId == WORKSPACE &&
                it.actorDefinitionId in listOf(SOURCE_DEFINITION, DESTINATION_DEFINITION)
            },
          )
        } returns ActorDefinitionUpdateResponse(REQUEST)
        business.filterIsInstance<ActorDefinitionAccessValidator>().forEach { every { it.validateWriteAccess(any()) } just Runs }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.requireActorDefinitionType(any(), any()) } throws AuthorizationResourceNotFoundException()
        every { data.requireActorDefinitionType(SOURCE_DEFINITION, true) } just Runs
        every { data.requireActorDefinitionType(DESTINATION_DEFINITION, false) } just Runs
        every { data.requireDefinitionDefaultVersion(any()) } just Runs
        if (missingDefault) every { data.requireDefinitionDefaultVersion(any()) } throws NoSuchElementException("default version missing")
        every { data.canUseActorDefinition(any(), any()) } throws
          AssertionError("Generic SPEC has actorDefinitionId=null; requested workspace is execution context, not a definition grant selector")
        if (missing) every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
        if (lookupError) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws
            java.io.IOException("actor definition execution context unavailable")
        }
        every { access.currentUserId("caller") } returns USER
        if (identityError) every { access.currentUserId("caller") } throws NoSuchElementException("current user missing")
        every { access.hasAccessibleCustomDefinition(USER, any()) } returns customAccess
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.workspaceId = if (scenario.create) WORKSPACE else UUID(0, 98)
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(DefinitionAccessService::class.java, access)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected == 200) {
          assertEquals("{\"requestId\":\"$REQUEST\"}", response.body())
        } else {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
      },
    )
  }

  private data class Scenario(
    val kind: String,
    val body: String,
    val create: Boolean,
    val rule: String,
  ) {
    val path = "/api/v1/actor_definitions/${if (create) "create" else "update"}"
  }

  private fun scenarios(): List<Scenario> =
    listOf("source", "destination").flatMap { kind ->
      listOf(
        Scenario(
          kind,
          """{"actorType":"$kind","workspaceId":"$WORKSPACE","actorDefinition":{"name":"fixture","dockerRepository":"fixture/image","dockerImageTag":"1","documentationUrl":"https://example.com"}}""",
          true,
          "io.airbyte.server.auth.ActorDefinitionCreateRule",
        ),
        Scenario(
          kind,
          """{"actorType":"$kind","actorDefinitionId":"${if (kind == "source") SOURCE_DEFINITION else DESTINATION_DEFINITION}","dockerImageTag":"2","workspaceId":"$WORKSPACE"}""",
          false,
          if (cloud) "io.airbyte.server.wrapped.auth.CloudActorDefinitionUpdateRule" else "io.airbyte.server.auth.ActorDefinitionUpdateRule",
        ),
      )
    }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val SOURCE_DEFINITION: UUID = UUID(0, 3)
    val DESTINATION_DEFINITION: UUID = UUID(0, 4)
    val USER: UUID = UUID(0, 5)
    val REQUEST: String = UUID(0, 9).toString()
  }
}
