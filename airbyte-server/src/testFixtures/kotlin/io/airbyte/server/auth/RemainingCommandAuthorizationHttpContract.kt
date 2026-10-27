/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
import io.airbyte.server.apis.controllers.CommandApiController
import io.airbyte.server.services.CommandService
import io.airbyte.server.services.CommandStatus
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingCommandAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `remaining command operations reject unsupported callers before command input output or credentials`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `command replication binds the saved job attempt to the requested connection before launch`() {
    val scenario = scenarios().single { it.path.endsWith("/run/replicate") }
    exercise(scenario, 400, wrongJobConnection = true, declaration = false)
  }

  @Test
  fun `existing command retries keep no op branches without reading unused tracking job or definition selectors`() {
    assertAll(
      scenarios().filter { it.run }.map { scenario ->
        Executable {
          val retry = if (scenario.suffix == "run/discover") scenario.copy(body = scenario.body.replace(",\"attemptNumber\":0", "")) else scenario
          exercise(retry, 200, existingCommand = true, unusedSelectors = true)
          exercise(scenario, 403, existingCommand = true, granted = false)
          exercise(scenario, 500, commandLookupError = true)
        }
      },
    )
  }

  @Test
  fun `all ten remaining command declarations execute their normal flow and exact saved scope policy`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 200, token = "application-owner")
      exercise(scenario, 200, token = "internal")
      exercise(scenario, 200, token = "scoped:$WORKSPACE")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, 403, granted = false)
      exercise(scenario, if (scenario.run) 404 else 403, missing = true)
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, 200, granted = false, security = false)
      exercise(scenario, if (scenario.path.endsWith("/output/check")) 200 else 403, token = "embedded-scoped:$WORKSPACE")
    }
  }

  @Test
  fun `all seven saved command operations keep forbidden for absent command ownership before business`() {
    assertAll(scenarios().filter { !it.run }.map { scenario -> Executable { exercise(scenario, 403, missing = true) } })
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    granted: Boolean = true,
    security: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    wrongJobConnection: Boolean = false,
    existingCommand: Boolean = false,
    unusedSelectors: Boolean = false,
    commandLookupError: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val executions = mockk<SavedExecutionAuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, listOf(CommandApiController::class.java), scenarios().map { it.rule }.toSet(), security).run(
      configure = { context, business ->
        val service = business.filterIsInstance<CommandService>().single()
        every { service.get(ID) } returns
          CommandService.CommandModel(
            ID,
            "workload",
            "SPEC",
            Jsons.jsonNode(mapOf("fixture" to true)),
            WORKSPACE,
            ORGANIZATION,
            java.time.OffsetDateTime.parse("2026-01-01T00:00:00Z"),
            java.time.OffsetDateTime.parse("2026-01-01T00:00:00Z"),
          )
        every { service.cancel(ID) } just Runs
        every { service.getStatus(ID) } returns CommandStatus.RUNNING
        every { service.getCheckJobOutput(ID, any()) } returns null
        every { service.getDiscoverJobOutput(ID, any()) } returns null
        every { service.getSpecJobOutput(ID, any()) } returns null
        every { service.getReplicationOutput(ID) } returns null
        every { service.createDiscoverCommand(ID, ACTOR, any(), any(), any(), any(), any()) } returns true
        every { service.createReplicateCommand(ID, CONNECTION, "17", 0L, any(), any(), any()) } returns true
        every { service.createSpecCommand(ID, DEFINITION, "1", WORKSPACE, any(), any()) } returns true
        business.filterIsInstance<RoleResolver>().forEach { roles ->
          val request = mockk<RoleResolver.Request>()
          every { roles.newRequest() } returns request
          every { request.withCurrentAuthentication() } returns request
          every { request.withRef(any(), any<UUID>()) } returns request
          every { request.requireOneOfRoles(any()) } just Runs
        }
        business.filterIsInstance<ActorRepository>().forEach {
          every { it.findByActorId(ACTOR) } returns
            mockk { every { workspaceId } returns WORKSPACE }
        }
        business.filterIsInstance<WorkspaceService>().forEach {
          every { it.getStandardWorkspaceFromConnection(CONNECTION, false) } returns
            StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(ORGANIZATION)
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.commandExistsInWorkspace(ID, owner) } returns false
        if (existingCommand) {
          every { data.commandExistsInWorkspace(ID, owner) } returns true
          every { service.createDiscoverCommand(ID, ACTOR, any(), any(), any(), any(), any()) } returns false
          every { service.createReplicateCommand(ID, CONNECTION, "17", 0L, any(), any(), any()) } returns false
          every { service.createSpecCommand(ID, DEFINITION, "1", WORKSPACE, any(), any()) } returns false
        }
        if (commandLookupError) {
          every { data.commandExistsInWorkspace(ID, owner) } throws
            java.io.IOException("command metadata unavailable")
        }
        every { executions.command(ID) } returns owner
        if (missing) every { executions.command(ID) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { executions.command(ID) } throws java.io.IOException("command owner unavailable")
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.actorWorkspaceAndOrganization(ACTOR) } returns owner
        every { data.connectionWorkspaceAndOrganization(CONNECTION) } returns owner
        every { data.savedAttemptConnectionId(17, 0) } returns if (wrongJobConnection) UUID(0, 99) else CONNECTION
        every { data.canUseActorDefinition(DEFINITION, owner) } returns true
        if (unusedSelectors) {
          every { data.savedAttemptConnectionId(17, 0) } throws AuthorizationResourceNotFoundException()
          every { data.canUseActorDefinition(DEFINITION, owner) } throws AuthorizationResourceNotFoundException()
        }
        if (missing) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
          every { data.actorWorkspaceAndOrganization(ACTOR) } throws AuthorizationResourceNotFoundException()
          every { data.connectionWorkspaceAndOrganization(CONNECTION) } throws AuthorizationResourceNotFoundException()
        }
        if (lookupError) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("owner unavailable")
          every { data.actorWorkspaceAndOrganization(ACTOR) } throws java.io.IOException("owner unavailable")
          every { data.connectionWorkspaceAndOrganization(CONNECTION) } throws java.io.IOException("owner unavailable")
        }
        every { grants.getPermissionsByAuthUserId("caller") } returns
          if (granted) {
            listOf(
              Permission().withWorkspaceId(WORKSPACE).withPermissionType(
                if (scenario.run ||
                  scenario.path.endsWith("/cancel")
                ) {
                  Permission.PermissionType.WORKSPACE_RUNNER
                } else {
                  Permission.PermissionType.WORKSPACE_READER
                },
              ),
            )
          } else {
            emptyList()
          }
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(SavedExecutionAuthorizationDataService::class.java, executions)
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
          assertEquals(ID, Jsons.deserialize(response.body()).get("id").asText())
          val service = business.filterIsInstance<CommandService>().single()
          io.mockk.verify(exactly = 1) {
            when (scenario.suffix) {
              "cancel" -> service.cancel(ID)
              "get" -> service.get(ID)
              "status" -> service.getStatus(ID)
              "output/check" -> service.getCheckJobOutput(ID, false)
              "output/discover" -> service.getDiscoverJobOutput(ID, false)
              "output/spec" -> service.getSpecJobOutput(ID, false)
              "output/replicate" -> service.getReplicationOutput(ID)
              "run/discover" -> service.createDiscoverCommand(ID, ACTOR, "tracking-uuid", if (unusedSelectors) null else 0L, any(), null, any())
              "run/replicate" -> service.createReplicateCommand(ID, CONNECTION, "17", 0L, null, null, any())
              "run/spec" -> service.createSpecCommand(ID, DEFINITION, "1", WORKSPACE, null, any())
            }
          }
        } else {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val ruleName: String,
    val body: String,
  ) {
    val path = "/api/v1/commands/$suffix"
    val rule = "io.airbyte.server.auth.$ruleName"
    val run = suffix.startsWith("run/")
  }

  private fun scenarios(): List<Scenario> =
    listOf(
      Scenario("cancel", "CancelCommandRule", """{"id":"$ID"}"""),
      Scenario("get", "ReadCommandRule", """{"id":"$ID"}"""),
      Scenario("status", "ReadCommandRule", """{"id":"$ID"}"""),
      Scenario("output/check", "ReadCheckCommandRule", """{"id":"$ID","with_logs":false}"""),
      Scenario("output/discover", "ReadCommandRule", """{"id":"$ID","with_logs":false}"""),
      Scenario("output/spec", "ReadCommandRule", """{"id":"$ID","with_logs":false}"""),
      Scenario("output/replicate", "ReadCommandRule", """{"id":"$ID"}"""),
      Scenario("run/discover", "RunDiscoverCommandRule", """{"id":"$ID","actor_id":"$ACTOR","jobId":"tracking-uuid","attemptNumber":0}"""),
      Scenario("run/replicate", "RunReplicateCommandRule", """{"id":"$ID","connection_id":"$CONNECTION","job_id":"17","attempt_number":0}"""),
      Scenario(
        "run/spec",
        "RunSpecCommandRule",
        """{"id":"$ID","actor_definition_id":"$DEFINITION","docker_image_tag":"1","workspace_id":"$WORKSPACE"}""",
      ),
    )

  companion object {
    const val ID = "fixture-command"
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val ACTOR: UUID = UUID(0, 3)
    val CONNECTION: UUID = UUID(0, 4)
    val DEFINITION: UUID = UUID(0, 5)
  }
}
