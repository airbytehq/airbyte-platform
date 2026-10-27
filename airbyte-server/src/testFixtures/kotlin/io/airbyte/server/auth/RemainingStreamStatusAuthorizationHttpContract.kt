/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.StreamStatusJobType
import io.airbyte.api.server.generated.models.StreamStatusRead
import io.airbyte.api.server.generated.models.StreamStatusReadList
import io.airbyte.api.server.generated.models.StreamStatusRunState
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.repositories.StreamStatusAuthorizationRepository
import io.airbyte.data.repositories.entities.StreamStatusAuthorization
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.StreamStatusesApiController
import io.airbyte.server.handlers.StreamStatusesHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

abstract class RemainingStreamStatusAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `stream status operations reject unsupported callers before status reads and writes`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `stream status lifecycle writes reject human and application owner administrators`() {
    assertAll(
      scenarios().filter { it.write }.flatMap { scenario ->
        listOf("user", "application-owner").map { token ->
          Executable { exercise(scenario, 403, token = token, grant = Permission.PermissionType.INSTANCE_ADMIN, declaration = false) }
        }
      },
    )
  }

  @Test
  fun `stream status writes and list bind the saved job attempt connection and workspace before business work`() {
    assertAll(
      scenarios().filter { it.suffix != "latest_per_run_state" }.flatMap { scenario ->
        listOf(
          Executable { exercise(scenario, 400, foreignJob = true, declaration = false) },
          Executable {
            exercise(
              scenario.copy(
                body =
                  scenario.body.replace(
                    WORKSPACE.toString(),
                    UUID(0, 99).toString(),
                  ),
              ),
              400,
              grant = Permission.PermissionType.INSTANCE_ADMIN,
              declaration = false,
            )
          },
        )
      },
    )
  }

  @Test
  fun `stream status updates cannot move a saved row between workspaces connections jobs attempts or job types`() {
    val update = scenarios().single { it.suffix == "update" }
    assertAll(
      listOf("workspace", "connection", "job", "attempt", "job-type", "missing", "wrong-id", "error").map { field ->
        Executable {
          exercise(
            update,
            when (field) {
              "workspace",
              "missing", "wrong-id",
              -> 404
              "error" -> 500
              else -> 400
            },
            savedRowCase = field,
            declaration = false,
          )
        }
      },
    )
  }

  @Test
  fun `stream status attempt-only filters preserve empty scoped queries and paired references require saved attempts`() {
    val listing = scenarios().single { it.suffix == "list" }
    exercise(
      listing.copy(
        body = """{"workspaceId":"$WORKSPACE","connectionId":"$CONNECTION","attemptNumber":-1,"pagination":{"pageSize":20,"rowOffset":0}}""",
      ),
      200,
    )
    exercise(
      listing.copy(
        body = """{"workspaceId":"$WORKSPACE","connectionId":"$CONNECTION","attemptNumber":999,"pagination":{"pageSize":20,"rowOffset":0}}""",
      ),
      200,
    )
    exercise(
      listing.copy(body = """{"workspaceId":"$WORKSPACE","connectionId":"$CONNECTION","jobId":$JOB,"pagination":{"pageSize":20,"rowOffset":0}}"""),
      200,
    )
    exercise(
      listing.copy(
        body =
          """{"workspaceId":"$WORKSPACE","connectionId":"$CONNECTION","jobId":$JOB,"attemptNumber":999,""" +
            """"pagination":{"pageSize":20,"rowOffset":0}}""",
      ),
      404,
    )
  }

  @Test
  fun `four stream status methods declare exact worker and reader rules and preserve normal results`() {
    for (scenario in scenarios()) {
      exercise(scenario, scenario.success)
      exercise(scenario, 403, token = "service", granted = false)
      exercise(
        scenario,
        scenario.success,
        token = "service",
        grant =
          if (scenario.write) {
            Permission.PermissionType.DATAPLANE
          } else {
            Permission.PermissionType.WORKSPACE_READER
          },
      )
      exercise(
        scenario,
        scenario.success,
        token = "dataplane",
        grant =
          if (scenario.write) {
            Permission.PermissionType.DATAPLANE
          } else {
            Permission.PermissionType.WORKSPACE_READER
          },
      )
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, scenario.success, granted = false, security = false)
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      if (scenario.write) {
        exercise(scenario, 403, token = "service", grant = Permission.PermissionType.WORKSPACE_READER)
      } else {
        exercise(scenario, 200, token = "internal")
        exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
        exercise(scenario, 200, token = "scoped:$WORKSPACE")
      }
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = if (scenario.write) "internal" else "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.WORKSPACE_READER,
    granted: Boolean = true,
    security: Boolean = true,
    foreignJob: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    savedRowCase: String = "",
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val statuses = mockk<StreamStatusAuthorizationRepository>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(StreamStatusesApiController::class.java),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet() + "io.airbyte.domain.services.auth.StreamStatusAuthorizationDataService",
      security,
    ).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<StreamStatusesHandler>().single()
        every { handler.createStreamStatus(any()) } returns read()
        every { handler.updateStreamStatus(any()) } returns read()
        every { handler.listStreamStatus(any()) } returns StreamStatusReadList(streamStatuses = emptyList())
        every { handler.listStreamStatusPerRunState(any()) } returns StreamStatusReadList(streamStatuses = emptyList())
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.workspaceAndOrganization(UUID(0, 99)) } returns WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
        every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns owner
        every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns owner
        every { data.replicationJobConnectionId(JOB) } returns if (foreignJob) UUID(0, 98) else CONNECTION
        every { data.savedAttemptConnectionId(JOB, any()) } throws AuthorizationResourceNotFoundException()
        every { data.savedAttemptConnectionId(JOB, 0) } returns if (foreignJob) UUID(0, 98) else CONNECTION
        val row = StreamStatusAuthorization(ROW, WORKSPACE, CONNECTION, JOB, 0, "sync")
        every { statuses.findByIdAndWorkspaceId(ROW, WORKSPACE) } returns
          when (savedRowCase) {
            "workspace" -> Optional.of(row.copy(workspaceId = UUID(0, 99)))
            "connection" -> Optional.of(row.copy(connectionId = UUID(0, 99)))
            "job" -> Optional.of(row.copy(jobId = JOB + 1))
            "attempt" -> Optional.of(row.copy(attemptNumber = 1))
            "job-type" -> Optional.of(row.copy(jobType = "reset"))
            "missing" -> Optional.empty()
            "wrong-id" -> Optional.of(row.copy(id = UUID(0, 99)))
            else -> Optional.of(row)
          }
        if (savedRowCase == "error") {
          every {
            statuses.findByIdAndWorkspaceId(
              ROW,
              WORKSPACE,
            )
          } throws java.io.IOException("status ownership projection unavailable")
        }
        if (missing) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
          every { data.savedConnectionActorIds(CONNECTION) } throws AuthorizationResourceNotFoundException()
        }
        if (lookupError) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("stream status workspace unavailable")
          every { data.savedConnectionActorIds(CONNECTION) } throws java.io.IOException("stream status connection unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        when (grant) {
          Permission.PermissionType.INSTANCE_ADMIN -> Unit
          Permission.PermissionType.DATAPLANE -> permission.organizationId = ORGANIZATION
          else -> permission.workspaceId = WORKSPACE
        }
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(StreamStatusAuthorizationRepository::class.java, statuses)
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
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != scenario.success) RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val body: String,
    val rule: String,
    val write: Boolean = false,
    val success: Int = 200,
  ) {
    val path = "/api/v1/stream_statuses/$suffix"
  }

  private fun scenarios(): List<Scenario> {
    val fields =
      """"attemptNumber":0,"connectionId":"$CONNECTION","jobId":$JOB,"jobType":"SYNC",""" +
        """"runState":"RUNNING","streamName":"orders","transitionedAt":0,"workspaceId":"$WORKSPACE""""
    return listOf(
      Scenario("create", "{$fields}", "StreamStatusCreateRule", true, 201),
      Scenario("update", "{\"id\":\"$ROW\",$fields}", "StreamStatusUpdateRule", true),
      Scenario(
        "list",
        """{"workspaceId":"$WORKSPACE","connectionId":"$CONNECTION","jobId":$JOB,"attemptNumber":0,"pagination":{"pageSize":20,"rowOffset":0}}""",
        "StreamStatusListRule",
      ),
      Scenario("latest_per_run_state", """{"connectionId":"$CONNECTION"}""", "StreamStatusRunStateRule"),
    )
  }

  private fun read() =
    StreamStatusRead(
      id = ROW,
      connectionId = CONNECTION,
      workspaceId = WORKSPACE,
      jobId = JOB,
      jobType = StreamStatusJobType.SYNC,
      attemptNumber = 0,
      streamName = "orders",
      transitionedAt = 0,
      runState = StreamStatusRunState.RUNNING,
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val CONNECTION: UUID = UUID(0, 3)
    val SOURCE: UUID = UUID(0, 4)
    val DESTINATION: UUID = UUID(0, 5)
    val ROW: UUID = UUID(0, 6)
    const val JOB = 47L
  }
}
