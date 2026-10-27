/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectorDocumentationRead
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectorDocumentationHandler
import io.airbyte.commons.server.handlers.DiagnosticToolHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.DiagnosticToolApiController
import io.airbyte.server.apis.controllers.ConnectorDocumentationApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File
import java.util.UUID

abstract class RemainingDocumentationDiagnosticAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `instance wide diagnostic report rejects organization and workspace readers before collection`() {
    exercise(diagnostic(), 403, grant = Permission.PermissionType.ORGANIZATION_READER, declaration = false)
    exercise(diagnostic(), 403, grant = Permission.PermissionType.WORKSPACE_READER, declaration = false)
    exercise(diagnostic(), 403, grant = Permission.PermissionType.ORGANIZATION_ADMIN, declaration = false)
  }

  @Test
  fun `connector documentation rejects a foreign consumed actor override and unsupported caller before fetching docs`() {
    exercise(docs(), 400, foreignActor = true, declaration = false)
    exercise(docs(), 403, token = "workload", declaration = false)
  }

  @Test
  fun `documentation diagnostic and Cloud unavailable routes execute exact edition policies and responses`() {
    for (scenario in scenarios()) {
      exercise(scenario, scenario.status)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "workload")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, if (scenario.unavailable) 404 else 200, granted = false, security = false)
    }
    exercise(docs(), 200, token = "scoped:$WORKSPACE")
    exercise(docs(), 404, missing = true)
    exercise(docs(), 500, lookupError = true)
    exercise(docs(), 403, usable = false)
    exercise(diagnostic(), 500, lookupError = true)
    exercise(docs().copy(body = docs().body.replace(",\"actorId\":\"$ACTOR\"", "")), 200)
    exercise(docs().copy(body = docs().body.replace("source", "destination")), 200)
  }

  @Test
  fun `Cloud unavailable instance configuration keeps authenticated denial after an earlier allow`() {
    for (scenario in scenarios().filter { it.unavailable }) exercise(scenario, 403, declaration = false)
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType = if (scenario.docs) Permission.PermissionType.WORKSPACE_READER else Permission.PermissionType.INSTANCE_ADMIN,
    granted: Boolean = true,
    foreignActor: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    usable: Boolean = true,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val report = File.createTempFile("plat1512-diagnostic", ".txt").apply { writeText("diagnostic fixture") }
    try {
      RemainingOperationHttpSupport(
        cloud,
        listOf(DiagnosticToolApiController::class.java, ConnectorDocumentationApiController::class.java) +
          if (cloud) listOf(Class.forName("io.airbyte.server.wrapped.InstanceConfigurationApiWrappedController")) else emptyList(),
        scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(),
        security,
      ).run(
        configure = { context, business ->
          for (instance in business) {
            when (instance) {
              is DiagnosticToolHandler -> every { instance.generateDiagnosticReport() } returns report
              is ConnectorDocumentationHandler ->
                every {
                  instance.getConnectorDocumentation(match { it.workspaceId == WORKSPACE && it.actorDefinitionId == DEFINITION })
                } returns ConnectorDocumentationRead("documentation fixture")
            }
          }
          val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
          val source = !scenario.body.contains("destination")
          every { data.workspaceAndOrganization(WORKSPACE) } returns owner
          every { data.requireActorDefinitionType(DEFINITION, source) } just Runs
          every { data.canUseActorDefinition(DEFINITION, owner) } returns usable
          every { data.savedSourceWorkspaceAndOrganization(ACTOR) } returns
            if (foreignActor) WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION) else owner
          every { data.savedDestinationWorkspaceAndOrganization(ACTOR) } returns
            if (foreignActor) WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION) else owner
          every { data.savedActorDefinitionId(ACTOR, WORKSPACE) } returns DEFINITION
          if (missing) every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
          if (lookupError &&
            scenario.docs
          ) {
            every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("documentation workspace unavailable")
          }
          val permission = Permission().withPermissionType(grant)
          if (grant.name.startsWith("WORKSPACE")) permission.workspaceId = WORKSPACE
          if (grant.name.startsWith("ORGANIZATION")) permission.organizationId = ORGANIZATION
          every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
          if (lookupError &&
            !scenario.docs
          ) {
            every { grants.getPermissionsByAuthUserId("caller") } throws java.io.IOException("diagnostic grants unavailable")
          }
          context.registerSingleton(AuthorizationDataService::class.java, data)
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
              scenario.path,
              "io.airbyte.server.auth.${scenario.rule}",
            )
          }
          val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
          assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
          if (expected != 200) {
            RemainingOperationHttpSupport.requireNoBusiness(business)
          } else if (scenario.docs) {
            assertEquals("{\"doc\":\"documentation fixture\"}", response.body())
          } else {
            assertEquals("diagnostic fixture", response.body())
          }
        },
      )
    } finally {
      report.delete()
    }
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val docs: Boolean = false,
    val method: String = "POST",
    val unavailable: Boolean = false,
    val status: Int = 200,
  )

  private fun diagnostic() = Scenario("/api/v1/diagnostic_tool/generate_report", """{"organizationId":"$ORGANIZATION"}""", "DiagnosticReportRule")

  private fun docs() =
    Scenario(
      "/api/v1/connector_documentation/get",
      """{"workspaceId":"$WORKSPACE","actorDefinitionId":"$DEFINITION","actorType":"source","actorId":"$ACTOR"}""",
      "ConnectorDocumentationRule",
      docs = true,
    )

  private fun scenarios() =
    listOf(diagnostic(), docs()) +
      if (cloud) {
        listOf(
          Scenario("/api/v1/instance_configuration", "", "UnavailableProtectedRule", method = "GET", unavailable = true, status = 403),
          Scenario(
            "/api/v1/instance_configuration/setup",
            """{"email":"fixture@example.com","anonymousDataCollection":false,"initialSetupComplete":true,"displaySetupWizard":false}""",
            "UnavailableProtectedRule",
            unavailable = true,
            status = 403,
          ),
        )
      } else {
        emptyList()
      }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
    val ACTOR: UUID = UUID(0, 4)
  }
}
