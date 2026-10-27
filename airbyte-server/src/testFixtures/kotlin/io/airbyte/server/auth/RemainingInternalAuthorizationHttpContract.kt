/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.JobsHandler
import io.airbyte.commons.server.handlers.SignalHandler
import io.airbyte.commons.server.handlers.dsr.DsrDeletionService
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.temporal.ConnectionManagerUtils
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.data.services.PermissionService
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.dsr.DsrManifest
import io.airbyte.server.apis.controllers.DsrDeletionController
import io.airbyte.server.apis.controllers.InternalApiController
import io.airbyte.server.apis.controllers.SignalApiController
import io.airbyte.workload.services.WorkloadService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingInternalAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `internal admin controls reject unsupported callers before signals recovery or deletion business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `recovery reports per connection results while DSR validates saved request existence`() {
    assertAll(
      scenarios().filter { it.savedRequest || it.rule == "ForceCleanupConnectionsRule" }.map { scenario ->
        Executable { exercise(scenario, if (scenario.savedRequest) 404 else scenario.success, missing = true, declaration = false) }
      },
    )
  }

  @Test
  fun `workflow completion signals reject human and application owner admin callers`() {
    val signal = scenarios().single { it.rule == "TerminalWorkflowSignalRule" }
    exercise(signal, 403, token = "user", declaration = false)
    exercise(signal, 403, token = "application-owner", declaration = false)
  }

  @Test
  fun `seven internal routes declare exact policies and retain real normal output and no auth behavior`() {
    for (scenario in scenarios()) {
      exercise(scenario, scenario.success)
      exercise(scenario, 403, token = "service", granted = false)
      exercise(
        scenario,
        403,
        token =
          if (scenario.rule ==
            "TerminalWorkflowSignalRule"
          ) {
            "service"
          } else {
            "user"
          },
        grant = Permission.PermissionType.ORGANIZATION_ADMIN,
      )
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "scoped:$WORKSPACE")
      exercise(scenario, scenario.success, granted = false, security = false)
      if (scenario.savedRequest || scenario.rule == "ForceCleanupConnectionsRule") {
        exercise(scenario, if (scenario.savedRequest) 500 else scenario.success, lookupError = true)
      }
      if (scenario.dsr) {
        exercise(scenario, 403, email = "outside@example.com")
        exercise(scenario, scenario.success, email = "  STAFF@AIRBYTE.COM  ")
      }
    }
    val cleanup = scenarios().single { it.rule == "ForceCleanupConnectionsRule" }
    exercise(cleanup.copy(body = """{"connection_ids":[]}"""), 200)
    val listing = scenarios().single { it.method == "GET" && !it.savedRequest }
    exercise(listing.copy(path = "/api/v1/internal/dsr?email=unknown%40example.com"), 200, emptyFilter = true)
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String =
      if (scenario.rule ==
        "TerminalWorkflowSignalRule"
      ) {
        "internal"
      } else {
        "user"
      },
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    email: String = "staff@airbyte.io",
    emptyFilter: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val requests = mockk<DataSubjectDeletionRequestRepository>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(InternalApiController::class.java, DsrDeletionController::class.java, SignalApiController::class.java),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet() + "io.airbyte.domain.services.auth.DsrAuthorizationDataService",
      security,
    ).run(
      configure = { context, business ->
        val normalizedEmail = email.trim().lowercase(java.util.Locale.US)
        val row =
          DataSubjectDeletionRequest(
            REQUEST,
            "target@example.com",
            "hash",
            "dg-fixture",
            DataSubjectDeletionStatus.previewed,
            requestedBy = normalizedEmail,
            oncallIssueNumber = "ONCALL-1",
            manifest = "{}",
          )
        val manifest =
          DsrManifest(
            "target@example.com",
            "dg-fixture",
            UUID(0, 8),
            null,
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
            0,
            0,
            emptyList(),
            emptyList(),
          )
        for (instance in business) {
          when (instance) {
            is CurrentUserService -> every { instance.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID(0, 9)).withEmail(email)
            is DsrDeletionService -> {
              every { instance.preview("target@example.com", "dg-fixture", "ONCALL-1", normalizedEmail) } returns
                DsrDeletionService.PreviewResult(REQUEST, "PREVIEWED", manifest, emptyList())
              every { instance.startExecution(REQUEST, "target@example.com", "dg-fixture", "ONCALL-1", normalizedEmail) } returns
                DsrDeletionService.StartExecutionResult(REQUEST, "RUNNING", false)
              every { instance.get(REQUEST) } returns row
              every { instance.listByEmail("target@example.com") } returns listOf(row)
              every { instance.listByEmail("unknown@example.com") } returns emptyList()
              every { instance.cancel(REQUEST, normalizedEmail) } returns row
            }
            is SignalHandler -> every { instance.signal(match { it.workflowId == "workflow-fixture" && it.workflowType == "sync" }) } just Runs
            is JobsHandler -> every { instance.failNonTerminalJobs(any()) } just Runs
            is ConnectionManagerUtils -> every { instance.safeTerminateWorkflow(any<UUID>(), "Force cleanup by admin via internal API") } just Runs
            is WorkloadService -> every { instance.getNonTerminalWorkloadsByConnection(any()) } returns emptyList()
          }
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.billingConnectionWorkspaceAndOrganization(CONNECTION) } returns owner
        every { data.billingConnectionWorkspaceAndOrganization(SECOND_CONNECTION) } returns owner
        every { requests.existsById(REQUEST) } returns !missing
        if (missing) every { data.billingConnectionWorkspaceAndOrganization(SECOND_CONNECTION) } throws AuthorizationResourceNotFoundException()
        if (lookupError) {
          every { requests.existsById(REQUEST) } throws java.io.IOException("request metadata unavailable")
          every { data.billingConnectionWorkspaceAndOrganization(SECOND_CONNECTION) } throws java.io.IOException("recovery ownership unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(DataSubjectDeletionRequestRepository::class.java, requests)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) {
          RemainingOperationHttpSupport.requireRule(
            context,
            scenario.method,
            scenario.template,
            "io.airbyte.server.auth.${scenario.rule}",
          )
        }
        val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != scenario.success) RemainingOperationHttpSupport.requireNoBusiness(business.filterNot { it is CurrentUserService })
        if (emptyFilter) assertEquals("{\"requests\":[]}", response.body())
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val success: Int,
    val method: String = "POST",
    val template: String = path.substringBefore('?'),
    val dsr: Boolean = false,
    val savedRequest: Boolean = false,
  )

  private fun scenarios(): List<Scenario> {
    val confirmation = """{"email":"target@example.com","datagrail_id":"dg-fixture","oncall_issue_number":"ONCALL-1"}"""
    val requestPath = "/api/v1/internal/dsr/$REQUEST"
    return listOf(
      Scenario(
        "/api/v1/internal/force_cleanup_connections",
        """{"connection_ids":["$CONNECTION","$SECOND_CONNECTION"]}""",
        "ForceCleanupConnectionsRule",
        200,
      ),
      Scenario("/api/v1/signal", """{"workflowType":"sync","workflowId":"workflow-fixture"}""", "TerminalWorkflowSignalRule", 200),
      Scenario("/api/v1/internal/dsr/preview", confirmation, "DsrPreviewRule", 200, dsr = true),
      Scenario(
        "$requestPath/execute",
        confirmation,
        "DsrSavedRequestRule",
        202,
        template = "/api/v1/internal/dsr/{requestId}/execute",
        dsr = true,
        savedRequest = true,
      ),
      Scenario(requestPath, "", "DsrSavedRequestRule", 200, "GET", "/api/v1/internal/dsr/{requestId}", dsr = true, savedRequest = true),
      Scenario("/api/v1/internal/dsr?email=target%40example.com", "", "DsrEmailFilterRule", 200, "GET", dsr = true),
      Scenario(
        "$requestPath/cancel",
        "{}",
        "DsrSavedRequestRule",
        200,
        template = "/api/v1/internal/dsr/{requestId}/cancel",
        dsr = true,
        savedRequest = true,
      ),
    )
  }

  companion object {
    val REQUEST: UUID = UUID(0, 1)
    val CONNECTION: UUID = UUID(0, 2)
    val SECOND_CONNECTION: UUID = UUID(0, 3)
    val WORKSPACE: UUID = UUID(0, 4)
    val ORGANIZATION: UUID = UUID(0, 5)
  }
}
