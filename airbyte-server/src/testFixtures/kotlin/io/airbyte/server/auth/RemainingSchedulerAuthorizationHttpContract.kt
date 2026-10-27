/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.SchedulerApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingSchedulerAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `inline scheduler operations reject unsupported callers before starting real checks`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `scheduler optional existing actor and definition relationships are checked before scheduler work`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 400, foreignActor = true, declaration = false) } })
  }

  @Test
  fun `all three scheduler routes execute independent actions and reject reader missing grants and foreign scope`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 200, noActor = true)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
      exercise(scenario, 200, token = "scoped:$WORKSPACE")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}", grant = Permission.PermissionType.INSTANCE_ADMIN)
      exercise(scenario, 403, usable = false)
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, 200, granted = false, security = false)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.source) {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR
      },
    foreignActor: Boolean = false,
    noActor: Boolean = false,
    usable: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(if (cloud) Class.forName("io.airbyte.server.wrapped.apis.SchedulerApiWrappedController") else SchedulerApiController::class.java),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(),
      security,
    ).run(
      configure = { context, business ->
        business.forEach(::configureDependency)
        val handler = business.filterIsInstance<SchedulerHandler>().single()
        every { handler.checkSourceConnectionFromSourceCreate(match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION }) } returns
          CheckConnectionRead(jobInfo = job(JobConfigType.CHECK_CONNECTION_SOURCE), status = CheckConnectionRead.Status.SUCCEEDED)
        every {
          handler.checkDestinationConnectionFromDestinationCreate(match { it.workspaceId == WORKSPACE && it.destinationDefinitionId == DEFINITION })
        } returns CheckConnectionRead(jobInfo = job(JobConfigType.CHECK_CONNECTION_DESTINATION), status = CheckConnectionRead.Status.SUCCEEDED)
        every {
          handler.discoverSchemaForSourceFromSourceCreate(
            match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION },
          )
        } returns
          SourceDiscoverSchemaRead(jobInfo = job(JobConfigType.DISCOVER_SCHEMA))
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.requireActorDefinitionType(DEFINITION, scenario.source) } just Runs
        every { data.canUseActorDefinition(DEFINITION, owner) } returns usable
        every { data.savedSourceWorkspaceAndOrganization(ACTOR) } returns
          if (foreignActor) {
            WorkspaceAndOrganization(
              UUID(
                0,
                99,
              ),
              ORGANIZATION,
            )
          } else {
            owner
          }
        every { data.savedDestinationWorkspaceAndOrganization(ACTOR) } returns
          if (foreignActor) {
            WorkspaceAndOrganization(
              UUID(
                0,
                99,
              ),
              ORGANIZATION,
            )
          } else {
            owner
          }
        every { data.savedActorDefinitionId(ACTOR, WORKSPACE) } returns DEFINITION
        if (missing) every { data.requireActorDefinitionType(DEFINITION, scenario.source) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.requireActorDefinitionType(DEFINITION, scenario.source) } throws java.io.IOException("definition unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.workspaceId = WORKSPACE
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(
            grants,
            RoleResolver(
              mockk(),
              mockk(),
              mockk(),
              mockk(),
            ),
            security,
          ),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val actorField = if (noActor) "" else "\"${if (scenario.source) "sourceId" else "destinationId"}\":\"$ACTOR\","
        val body =
          """{$actorField"workspaceId":"$WORKSPACE",""" +
            """"${if (scenario.source) "sourceDefinitionId" else "destinationDefinitionId"}":"$DEFINITION",""" +
            """"connectionConfiguration":{}}"""
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != 200) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        } else {
          assertTrue(response.body().contains("\"jobInfo\""), response.body())
        }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val rule: String,
    val source: Boolean,
  )

  private fun scenarios() =
    listOf(
      Scenario("/api/v1/scheduler/sources/check_connection", "SchedulerSourceRule", true),
      Scenario("/api/v1/scheduler/sources/discover_schema", "SchedulerSourceRule", true),
      Scenario("/api/v1/scheduler/destinations/check_connection", "SchedulerDestinationRule", false),
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
    val ACTOR: UUID = UUID(0, 4)

    fun job(type: JobConfigType): SynchronousJobRead =
      SynchronousJobRead(id = UUID(0, 5), configType = type, createdAt = 0L, endedAt = 0L, succeeded = true)
  }
}
