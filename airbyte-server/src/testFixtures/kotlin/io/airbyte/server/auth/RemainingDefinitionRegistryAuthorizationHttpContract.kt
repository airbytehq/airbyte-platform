/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionReadList
import io.airbyte.api.server.generated.models.EnterpriseConnectorStubsReadList
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionRead
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionReadList
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionRead
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionReadList
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceDefinitionReadList
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.EnterpriseConnectorStubsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.DestinationDefinitionApiController
import io.airbyte.server.apis.controllers.SourceDefinitionApiController
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

abstract class RemainingDefinitionRegistryAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `definition registry operations reject unsupported tokens before normal metadata or grant business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `all twenty one registry operations execute independently expected rules and valid normal responses`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, scenario.status) } })
  }

  @Test
  fun `registry explicit scoped and global actions reject missing grants while authentication metadata stays grant independent`() {
    for (scenario in scenarios()) {
      exercise(scenario, if (scenario.authOnly) scenario.status else 403, granted = false)
      if (!scenario.authOnly) exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER, foreignGrant = scenario.workspaceRead)
      exercise(scenario, scenario.status, granted = false, security = false)
    }
  }

  @Test
  fun `typed definitions and required workspace failures reject before business rather than global fallback`() {
    for (scenario in scenarios().filter { it.savedDefinition || it.workspaceRead || it.grantTarget }) {
      exercise(scenario, 404, missing = true, declaration = false)
      exercise(scenario, 500, lookupError = true, declaration = false)
    }
  }

  @Test
  fun `scoped definition availability is required before reading while grant creation can establish availability`() {
    for (scenario in scenarios().filter { it.rule.endsWith("ScopedReadRule") }) exercise(scenario, 404, usable = false, declaration = false)
    for (scenario in scenarios().filter { it.grantTarget }) exercise(scenario, 200, usable = false)
  }

  @Test
  fun `private registry lists and grant revocations retain missing target filter behavior`() {
    for (scenario in scenarios().filter { it.path.endsWith("/list_private") || it.path.endsWith("/revoke_definition") }) {
      exercise(scenario, scenario.status, unknownFilter = true)
    }
  }

  @Test
  fun `registry organization scopes exact caller roles and workspace token limits remain distinct`() {
    for (scenario in scenarios()) {
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, if (scenario.workspaceRead) scenario.status else 403, token = "scoped:$WORKSPACE")
      if (scenario.path.endsWith("/get_for_scope") || scenario.grantTarget || scenario.path.endsWith("/revoke_definition")) {
        val organization =
          scenario.copy(
            body =
              scenario.body
                .replace(
                  WORKSPACE.toString(),
                  ORGANIZATION.toString(),
                ).replace("\"workspace\"", "\"organization\""),
          )
        if (scenario.rule.endsWith("ScopedReadRule")) {
          exercise(organization, 200, grant = Permission.PermissionType.ORGANIZATION_READER)
          exercise(organization, 403, grant = Permission.PermissionType.WORKSPACE_ADMIN)
          exercise(organization, 404, usable = false, grant = Permission.PermissionType.ORGANIZATION_READER)
        } else {
          exercise(organization, scenario.status)
        }
        if (scenario.grantTarget) exercise(organization, 404, missing = true)
      }
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType = scenario.grant,
    foreignGrant: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    usable: Boolean = true,
    unknownFilter: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val controllers =
      if (cloud) {
        listOf(
          Class.forName("io.airbyte.server.wrapped.apis.SourceDefinitionApiWrappedController"),
          Class.forName("io.airbyte.server.wrapped.apis.DestinationDefinitionApiWrappedController"),
        )
      } else {
        listOf(SourceDefinitionApiController::class.java, DestinationDefinitionApiController::class.java)
      }
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(), security).run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is SourceDefinitionsHandler -> {
              every { instance.getSourceDefinition(DEFINITION, true) } returns source()
              every {
                instance.getSourceDefinitionForScope(
                  match {
                    it.actorDefinitionId == DEFINITION && it.scopeId in
                      setOf(
                        WORKSPACE,
                        ORGANIZATION,
                      )
                  },
                )
              } returns source()
              every { instance.getSourceDefinitionForWorkspace(match { it.sourceDefinitionId == DEFINITION && it.workspaceId == WORKSPACE }) } returns
                source()
              every { instance.listSourceDefinitions() } returns SourceDefinitionReadList(emptyList())
              every { instance.listLatestSourceDefinitions() } returns SourceDefinitionReadList(emptyList())
              every { instance.listSourceDefinitionsForWorkspace(match { it.workspaceId == WORKSPACE }) } returns
                SourceDefinitionReadList(emptyList())
              every { instance.listPrivateSourceDefinitions(match { it.workspaceId == WORKSPACE }) } returns
                PrivateSourceDefinitionReadList(emptyList())
              every { instance.listPrivateSourceDefinitions(match { it.workspaceId == UNKNOWN }) } returns
                PrivateSourceDefinitionReadList(emptyList())
              every {
                instance.grantSourceDefinitionToWorkspaceOrOrganization(
                  match {
                    it.actorDefinitionId == DEFINITION && it.scopeId in
                      setOf(
                        WORKSPACE,
                        ORGANIZATION,
                      )
                  },
                )
              } returns
                PrivateSourceDefinitionRead(
                  source(),
                  true,
                )
              every {
                instance.revokeSourceDefinition(
                  match {
                    it.actorDefinitionId == DEFINITION && it.scopeId in
                      setOf(
                        WORKSPACE,
                        ORGANIZATION,
                      )
                  },
                )
              } just Runs
              every { instance.revokeSourceDefinition(match { it.actorDefinitionId == UNKNOWN && it.scopeId == UNKNOWN }) } just Runs
            }
            is DestinationDefinitionsHandler -> {
              every { instance.getDestinationDefinition(DEFINITION, true) } returns destination()
              every {
                instance.getDestinationDefinitionForScope(
                  match {
                    it.actorDefinitionId == DEFINITION && it.scopeId in
                      setOf(
                        WORKSPACE,
                        ORGANIZATION,
                      )
                  },
                )
              } returns destination()
              every {
                instance.getDestinationDefinitionForWorkspace(
                  match { it.destinationDefinitionId == DEFINITION && it.workspaceId == WORKSPACE },
                )
              } returns
                destination()
              every { instance.listDestinationDefinitions() } returns DestinationDefinitionReadList(emptyList())
              every { instance.listLatestDestinationDefinitions() } returns DestinationDefinitionReadList(emptyList())
              every { instance.listDestinationDefinitionsForWorkspace(match { it.workspaceId == WORKSPACE }) } returns
                DestinationDefinitionReadList(emptyList())
              every { instance.listPrivateDestinationDefinitions(match { it.workspaceId == WORKSPACE }) } returns
                PrivateDestinationDefinitionReadList(emptyList())
              every { instance.listPrivateDestinationDefinitions(match { it.workspaceId == UNKNOWN }) } returns
                PrivateDestinationDefinitionReadList(emptyList())
              every {
                instance.grantDestinationDefinitionToWorkspaceOrOrganization(
                  match {
                    it.actorDefinitionId == DEFINITION &&
                      it.scopeId in
                      setOf(
                        WORKSPACE,
                        ORGANIZATION,
                      )
                  },
                )
              } returns
                PrivateDestinationDefinitionRead(
                  destination(),
                  true,
                )
              every {
                instance.revokeDestinationDefinition(
                  match {
                    it.actorDefinitionId == DEFINITION && it.scopeId in
                      setOf(
                        WORKSPACE,
                        ORGANIZATION,
                      )
                  },
                )
              } just Runs
              every { instance.revokeDestinationDefinition(match { it.actorDefinitionId == UNKNOWN && it.scopeId == UNKNOWN }) } just Runs
            }
            is EnterpriseConnectorStubsHandler -> {
              every { instance.listEnterpriseSourceStubs() } returns EnterpriseConnectorStubsReadList(emptyList())
              every { instance.listEnterpriseSourceStubsForWorkspace(WORKSPACE) } returns EnterpriseConnectorStubsReadList(emptyList())
              every { instance.listEnterpriseDestinationStubsForWorkspace(WORKSPACE) } returns EnterpriseConnectorStubsReadList(emptyList())
            }
          }
        }
        val source = scenario.path.contains("source_definitions")
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } returns owner
        every { data.requireActorDefinitionType(DEFINITION, source) } just Runs
        every { data.requireDefinitionDefaultVersion(DEFINITION) } just Runs
        every { data.requireActorDefinitionTypeIfPresent(DEFINITION, source) } just Runs
        every { data.requireActorDefinitionTypeIfPresent(UNKNOWN, source) } just Runs
        every { data.canUseActorDefinition(DEFINITION, owner) } returns usable
        every { data.organizationCanUseActorDefinition(DEFINITION, ORGANIZATION) } returns usable
        every { data.workspaceAndOrganization(UNKNOWN) } throws AuthorizationResourceNotFoundException()
        if (missing || lookupError) {
          val failure = if (missing) AuthorizationResourceNotFoundException() else java.io.IOException("registry metadata unavailable")
          every { data.organization(ORGANIZATION) } throws failure
          every { data.workspaceAndOrganization(WORKSPACE) } throws failure
          every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } throws failure
          every { data.requireActorDefinitionType(DEFINITION, source) } throws failure
        }
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("WORKSPACE")) permission.workspaceId = if (foreignGrant) UUID(0, 99) else WORKSPACE
        if (grant.name.startsWith("ORGANIZATION")) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
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
        val body =
          if (unknownFilter) {
            scenario.body.replace(WORKSPACE.toString(), UNKNOWN.toString()).replace(DEFINITION.toString(), UNKNOWN.toString())
          } else {
            scenario.body
          }
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.headers().map()} ${response.body()}")
        if (expected !in setOf(200, 204)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200 && !scenario.empty) assertTrue(response.body().isNotBlank())
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val grant: Permission.PermissionType = Permission.PermissionType.WORKSPACE_READER,
    val authOnly: Boolean = false,
    val workspaceRead: Boolean = false,
    val savedDefinition: Boolean = false,
    val grantTarget: Boolean = false,
    val status: Int = 200,
    val empty: Boolean = false,
  )

  private fun scenarios(): List<Scenario> {
    val workspace = """{"workspaceId":"$WORKSPACE"}"""
    val scope = """{"actorDefinitionId":"$DEFINITION","scopeId":"$WORKSPACE","scopeType":"workspace"}"""
    val admin = Permission.PermissionType.INSTANCE_ADMIN
    return listOf(
      Scenario(
        "/api/v1/source_definitions/get",
        """{"sourceDefinitionId":"$DEFINITION"}""",
        "SourceRegistryMetadataRule",
        authOnly = true,
        savedDefinition = true,
      ),
      Scenario("/api/v1/source_definitions/list", "{}", "AuthenticatedMetadataRule", authOnly = true),
      Scenario("/api/v1/source_definitions/list_latest", "{}", "AuthenticatedMetadataRule", authOnly = true),
      Scenario("/api/v1/source_definitions/list_enterprise_source_stubs", "{}", "AuthenticatedMetadataRule", authOnly = true),
      Scenario("/api/v1/source_definitions/list_for_workspace", workspace, "DefinitionRegistryWorkspaceReadRule", workspaceRead = true),
      Scenario(
        "/api/v1/source_definitions/list_enterprise_stubs_for_workspace",
        workspace,
        "DefinitionRegistryWorkspaceReadRule",
        workspaceRead = true,
      ),
      Scenario("/api/v1/source_definitions/get_for_scope", scope, "SourceRegistryScopedReadRule", workspaceRead = true, savedDefinition = true),
      Scenario(
        "/api/v1/source_definitions/get_for_workspace",
        """{"sourceDefinitionId":"$DEFINITION","workspaceId":"$WORKSPACE"}""",
        "SourceRegistryScopedReadRule",
        workspaceRead = true,
        savedDefinition = true,
      ),
      Scenario("/api/v1/source_definitions/list_private", workspace, "DefinitionRegistryPrivateListRule", admin),
      Scenario("/api/v1/source_definitions/grant_definition", scope, "SourceRegistryGrantRule", admin, savedDefinition = true, grantTarget = true),
      Scenario("/api/v1/source_definitions/revoke_definition", scope, "SourceRegistryRevokeRule", admin, status = 204, empty = true),
      Scenario(
        "/api/v1/destination_definitions/get",
        """{"destinationDefinitionId":"$DEFINITION"}""",
        "DestinationRegistryMetadataRule",
        authOnly = true,
        savedDefinition = true,
      ),
      Scenario("/api/v1/destination_definitions/list", "{}", "AuthenticatedMetadataRule", authOnly = true),
      Scenario("/api/v1/destination_definitions/list_latest", "{}", "AuthenticatedMetadataRule", authOnly = true),
      Scenario("/api/v1/destination_definitions/list_for_workspace", workspace, "DefinitionRegistryWorkspaceReadRule", workspaceRead = true),
      Scenario(
        "/api/v1/destination_definitions/list_enterprise_stubs_for_workspace",
        workspace,
        "DefinitionRegistryWorkspaceReadRule",
        workspaceRead = true,
      ),
      Scenario(
        "/api/v1/destination_definitions/get_for_scope",
        scope,
        "DestinationRegistryScopedReadRule",
        workspaceRead = true,
        savedDefinition = true,
      ),
      Scenario(
        "/api/v1/destination_definitions/get_for_workspace",
        """{"destinationDefinitionId":"$DEFINITION","workspaceId":"$WORKSPACE"}""",
        "DestinationRegistryScopedReadRule",
        workspaceRead = true,
        savedDefinition = true,
      ),
      Scenario("/api/v1/destination_definitions/list_private", workspace, "DefinitionRegistryPrivateListRule", admin),
      Scenario(
        "/api/v1/destination_definitions/grant_definition",
        scope,
        "DestinationRegistryGrantRule",
        admin,
        savedDefinition = true,
        grantTarget = true,
      ),
      Scenario("/api/v1/destination_definitions/revoke_definition", scope, "DestinationRegistryRevokeRule", admin, empty = true),
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
    val UNKNOWN: UUID = UUID(0, 99)

    fun source() = SourceDefinitionRead(DEFINITION, "fixture", "fixture", "1.0")

    fun destination() = DestinationDefinitionRead(DEFINITION, "fixture", "fixture", "1.0", java.net.URI("https://example.com/fixture"))
  }
}
