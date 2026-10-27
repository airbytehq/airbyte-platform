/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ActorDefinitionResultResponse
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.temporal.scheduling.ActorDefinitionWorkflowOwner
import io.airbyte.config.ActorType
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.DefinitionAccessService
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
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
import java.util.Optional
import java.util.UUID

abstract class RemainingActorAsyncAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `async definition finish and result reject workload credentials before mutation or workflow result reads`() {
    assertAll(listOf(true, false).map { finish -> Executable { exercise(finish, 403, token = "workload") } })
  }

  @Test
  fun `finish binds nested definition kind and optional SPEC command to saved execution workspace`() {
    exercise(true, 400, foreignCommand = true)
    exercise(true, 404, wrongKind = true)
    exercise(true, 200, hasDefinition = false, hasCommand = false)
    if (cloud) exercise(true, 422, customAccess = false)
  }

  @Test
  fun `Cloud finish keeps missing current user authentication errors`() {
    if (cloud) exercise(true, 401, identityError = true)
  }

  @Test
  fun `finish and result declare concrete rules and preserve exact scoped caller and error behavior`() {
    for (finish in listOf(true, false)) {
      exercise(finish, 200)
      exercise(finish, 200, token = "application-owner")
      exercise(finish, 200, token = "internal")
      exercise(finish, 200, token = "scoped:$WORKSPACE")
      exercise(finish, 403, token = "scoped:${UUID(0, 99)}")
      exercise(finish, 403, token = "embedded-scoped:$WORKSPACE")
      exercise(finish, 403, granted = false)
      exercise(finish, 404, missing = true)
      exercise(finish, 500, lookupError = true)
      exercise(finish, 200, granted = false, security = false)
    }
    exercise(false, 404, missingWorkflow = true)
    exercise(false, 200, hasDefinition = false)
  }

  private fun exercise(
    finish: Boolean,
    expected: Int,
    token: String = "user",
    granted: Boolean = true,
    security: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    foreignCommand: Boolean = false,
    wrongKind: Boolean = false,
    hasDefinition: Boolean = true,
    hasCommand: Boolean = true,
    customAccess: Boolean = true,
    missingWorkflow: Boolean = false,
    identityError: Boolean = false,
  ) {
    val controller =
      if (cloud) {
        Class.forName("io.airbyte.server.wrapped.apis.ActorDefinitionApiWrappedController")
      } else {
        ActorDefinitionApiController::class.java
      }
    val finishRule =
      if (cloud) {
        "io.airbyte.server.wrapped.auth.CloudActorDefinitionFinishRule"
      } else {
        "io.airbyte.server.auth.ActorDefinitionFinishRule"
      }
    val resultRule = "io.airbyte.server.auth.ActorDefinitionResultRule"
    val data = mockk<AuthorizationDataService>()
    val executions = mockk<SavedExecutionAuthorizationDataService>()
    val temporal = mockk<TemporalClient>()
    val grants = mockk<PermissionService>()
    val access = mockk<DefinitionAccessService>()
    RemainingOperationHttpSupport(cloud, listOf(controller), setOf(finishRule, resultRule), security).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<ActorDefinitionsHandler>().single()
        every { handler.finishActorDefinitionUpdate(any(), any(), any(), any(), WORKSPACE) } returns
          ActorDefinitionsHandler.ActorDefinitionFinishResult(DEFINITION, null)
        every { handler.getActorDefinitionResult("request") } returns ActorDefinitionResultResponse(ActorDefinitionResultResponse.Status.RUNNING)
        business.filterIsInstance<ActorDefinitionAccessValidator>().forEach { every { it.validateWriteAccess(any()) } just Runs }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        if (missing) every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("owner unavailable")
        every { data.requireActorDefinitionType(DEFINITION, true) } just Runs
        if (wrongKind) every { data.requireActorDefinitionType(DEFINITION, true) } throws AuthorizationResourceNotFoundException()
        every { executions.specCommand("spec-command") } returns if (foreignCommand) WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION) else owner
        every { temporal.getActorDefinitionWorkflowOwner("request") } returns
          ActorDefinitionWorkflowOwner(WORKSPACE, ActorType.SOURCE, if (hasDefinition) Optional.of(DEFINITION) else Optional.empty())
        if (missingWorkflow) every { temporal.getActorDefinitionWorkflowOwner("request") } throws NoSuchElementException()
        every { grants.getPermissionsByAuthUserId("caller") } returns
          if (granted) {
            listOf(
              Permission()
                .withWorkspaceId(
                  WORKSPACE,
                ).withPermissionType(if (finish) Permission.PermissionType.WORKSPACE_SOURCE_EDITOR else Permission.PermissionType.WORKSPACE_READER),
            )
          } else {
            emptyList()
          }
        every { access.currentUserId("caller") } returns UUID(0, 4)
        if (identityError) every { access.currentUserId("caller") } throws NoSuchElementException("current user missing")
        every { access.hasAccessibleCustomDefinition(UUID(0, 4), DEFINITION) } returns customAccess
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(SavedExecutionAuthorizationDataService::class.java, executions)
        context.registerSingleton(TemporalClient::class.java, temporal)
        context.registerSingleton(DefinitionAccessService::class.java, access)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        val path = "/api/v1/actor_definitions/${if (finish) "finish" else "result"}"
        RemainingOperationHttpSupport.requireRule(context, "POST", path, if (finish) finishRule else resultRule)
        val definition = if (hasDefinition) ",\"actorDefinitionId\":\"$DEFINITION\"" else ""
        val command = if (hasCommand) ",\"commandId\":\"spec-command\"" else ""
        val body =
          if (finish) {
            """{"workspaceId":"$WORKSPACE","actorUpdateRequest":{"actorType":"source","imageName":"fixture/image","imageTag":"1"$definition},"metadata":{"name":"fixture"}$command}"""
          } else {
            """{"requestId":"request"}"""
          }
        val response = RemainingOperationHttpSupport.request(server, "POST", path, body, token)
        assertEquals(expected, response.statusCode(), "$path: ${response.body()}")
        if (expected == 200) {
          assertEquals(if (finish) "{\"actorDefinitionId\":\"$DEFINITION\"}" else "{\"status\":\"running\"}", response.body())
        } else {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
      },
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
  }
}
