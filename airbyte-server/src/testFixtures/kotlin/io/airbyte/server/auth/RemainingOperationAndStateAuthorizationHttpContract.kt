/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectionState
import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.api.server.generated.models.OperationRead
import io.airbyte.api.server.generated.models.OperationReadList
import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.api.server.generated.models.OperatorType
import io.airbyte.api.server.generated.models.OperatorWebhook
import io.airbyte.api.server.generated.models.OperatorWebhookDbtCloud
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.StateHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.OperationApiController
import io.airbyte.server.apis.controllers.StateApiController
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

abstract class RemainingOperationAndStateAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `operation and state routes reject unsupported credentials before normal business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `all operation and state routes execute independently expected rules and normal responses`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, scenario.status) } })
  }

  @Test
  fun `all operation and state routes reject absent grants before business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, granted = false) } })
  }

  @Test
  fun `operation lists validate every saved child and write actions reject actual readers`() {
    val list = scenarios().single { it.path == "/api/v1/operations/list" }
    exercise(list, 400, foreignChild = true, declaration = false)
    for (scenario in scenarios().filter {
      it.grant != Permission.PermissionType.WORKSPACE_READER
    }) {
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER, declaration = false)
    }
  }

  @Test
  fun `worker state mutation rejects human administrators and supports actual worker grants`() {
    val write = scenarios().single { it.path == "/api/v1/state/create_or_update" }
    for (token in listOf(
      "user",
      "application-owner",
    )) {
      exercise(write, 403, token = token, grant = Permission.PermissionType.INSTANCE_ADMIN, declaration = false)
    }
    for (token in listOf("internal", "dataplane", "service")) exercise(write, 200, token = token)
    exercise(write, 403, token = "service", granted = false)
  }

  @Test
  fun `operation and state selectors preserve lookup errors scopes empty state and configured no auth`() {
    for (scenario in scenarios()) {
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      if (!scenario.worker) {
        exercise(scenario, scenario.status, token = "scoped:$WORKSPACE")
        exercise(scenario, 403, token = "scoped:${UUID(0, 99)}", grant = Permission.PermissionType.INSTANCE_ADMIN)
      }
      exercise(scenario, scenario.status, granted = false, security = false)
    }
    val state = scenarios().single { it.path == "/api/v1/state/create_or_update_safe" }
    // StateConverter discards the nested connectionId; only the outer ID is used by persistence.
    exercise(state.copy(body = """{"connectionId":"$CONNECTION","connectionState":{"connectionId":"${UUID(0, 99)}","stateType":"not_set"}}"""), 200)
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = if (scenario.worker) "service" else "user",
    granted: Boolean = true,
    grant: Permission.PermissionType = scenario.grant,
    declaration: Boolean = true,
    foreignChild: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
  ) {
    val controllers =
      listOf(
        if (cloud) Class.forName("io.airbyte.server.wrapped.apis.OperationApiWrappedController") else OperationApiController::class.java,
        if (cloud) Class.forName("io.airbyte.server.wrapped.apis.StateApiWrappedController") else StateApiController::class.java,
      )
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(), security).run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is OperationsHandler -> {
              every { instance.createOperation(match { it.workspaceId == WORKSPACE }) } returns operation()
              every { instance.getOperation(match { it.operationId == OPERATION }) } returns operation()
              every { instance.updateOperation(match { it.operationId == OPERATION }) } returns operation()
              every { instance.deleteOperation(match { it.operationId == OPERATION }) } just Runs
              every { instance.listOperationsForConnection(match { it.connectionId == CONNECTION }) } returns
                OperationReadList(operations = emptyList())
            }
            is StateHandler -> {
              every { instance.getState(match { it.connectionId == CONNECTION }) } returns state()
              every { instance.createOrUpdateState(match { it.connectionId == CONNECTION }) } returns state()
              every { instance.createOrUpdateStateSafe(match { it.connectionId == CONNECTION }) } returns state()
            }
          }
          configureDependency(instance)
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.operationWorkspaceAndOrganization(OPERATION) } answers { data.workspaceAndOrganization(WORKSPACE) }
        every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } answers { data.workspaceAndOrganization(WORKSPACE) }
        every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } answers { data.workspaceAndOrganization(WORKSPACE) }
        every { data.savedConnectionOperationIds(CONNECTION) } returns listOf(OPERATION, OPERATION)
        if (foreignChild) every { data.operationWorkspaceAndOrganization(OPERATION) } returns WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
        if (missing) every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("saved owner unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.workspaceId = WORKSPACE
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected !in setOf(200, 204)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200) assertTrue(response.body().isNotBlank())
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val grant: Permission.PermissionType,
    val status: Int = 200,
    val worker: Boolean = false,
  )

  private fun scenarios(): List<Scenario> =
    listOf(
      Scenario(
        "/api/v1/operations/create",
        """{"workspaceId":"$WORKSPACE","name":"fixture","operatorConfiguration":{"operatorType":"webhook","webhook":{}}}""",
        "SavedOperationWriteRule",
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ),
      Scenario("/api/v1/operations/get", """{"operationId":"$OPERATION"}""", "SavedOperationReadRule", Permission.PermissionType.WORKSPACE_READER),
      Scenario(
        "/api/v1/operations/update",
        """{"operationId":"$OPERATION","name":"fixture","operatorConfiguration":{"operatorType":"webhook","webhook":{}}}""",
        "SavedOperationWriteRule",
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
      ),
      Scenario(
        "/api/v1/operations/delete",
        """{"operationId":"$OPERATION"}""",
        "SavedOperationWriteRule",
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        204,
      ),
      Scenario("/api/v1/operations/list", """{"connectionId":"$CONNECTION"}""", "SavedOperationListRule", Permission.PermissionType.WORKSPACE_READER),
      Scenario("/api/v1/state/get", """{"connectionId":"$CONNECTION"}""", "ConnectionStateReadRule", Permission.PermissionType.WORKSPACE_READER),
      Scenario(
        "/api/v1/state/create_or_update_safe",
        """{"connectionId":"$CONNECTION","connectionState":{"connectionId":"$CONNECTION","stateType":"not_set"}}""",
        "ConnectionStateWriteRule",
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ),
      Scenario(
        "/api/v1/state/create_or_update",
        """{"connectionId":"$CONNECTION","connectionState":{"connectionId":"$CONNECTION","stateType":"not_set"}}""",
        "ConnectionStateWorkerRule",
        Permission.PermissionType.DATAPLANE,
        worker = true,
      ),
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val OPERATION: UUID = UUID(0, 3)
    val CONNECTION: UUID = UUID(0, 4)
    val SOURCE: UUID = UUID(0, 5)
    val DESTINATION: UUID = UUID(0, 6)

    fun state(): ConnectionState = ConnectionState(connectionId = CONNECTION, stateType = ConnectionStateType.NOT_SET)

    fun operation(): OperationRead =
      OperationRead(
        workspaceId = WORKSPACE,
        operationId = OPERATION,
        name = "fixture",
        operatorConfiguration =
          OperatorConfiguration(
            operatorType = OperatorType.WEBHOOK,
            webhook =
              OperatorWebhook(
                webhookType = OperatorWebhook.WebhookType.DBT_CLOUD,
                dbtCloud = OperatorWebhookDbtCloud(accountId = 1L, jobId = 2L),
              ),
          ),
      )
  }
}
