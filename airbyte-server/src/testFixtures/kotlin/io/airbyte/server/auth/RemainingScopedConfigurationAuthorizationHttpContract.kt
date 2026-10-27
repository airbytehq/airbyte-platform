/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ScopedConfigurationContextResponse
import io.airbyte.api.server.generated.models.ScopedConfigurationRead
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ScopedConfigurationHandler
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ScopedConfigurationMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.ScopedConfigurationApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

abstract class RemainingScopedConfigurationAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `scoped configuration rejects unsupported callers before reading enriched configs or writing pins`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `scoped config required saved scope and nested new scope are validated before handler work`() {
    for (scenario in scenarios().filter { it.references || it.saved }) {
      exercise(
        scenario,
        if (scenario.create || scenario.update) 400 else 404,
        missing = true,
        declaration = false,
      )
    }
  }

  @Test
  fun `all six scoped configuration methods execute independent global admin policies and normal typed results`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, grant = Permission.PermissionType.ORGANIZATION_ADMIN)
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "scoped:$WORKSPACE")
      exercise(scenario, 200, granted = false, security = false)
      if (scenario.references || scenario.saved) exercise(scenario, 500, lookupError = true)
    }
  }

  @Test
  fun `scoped configuration validates saved and nested semantic version parents and preserves explicit upsert`() {
    for (scenario in scenarios().filter { it.create || it.update || it.rule == "ScopedConfigurationReadRule" }) {
      exercise(scenario, if (scenario.create || scenario.update) 400 else 404, foreignVersion = true)
    }
    exercise(scenarios().single { it.update }, 200, absent = true)
    for (scenario in scenarios().filter { it.saved && !it.update }) exercise(scenario, 404, absent = true)
  }

  @Test
  fun `global config query filters allow empty unknown scopes and keys while actor context requires saved ancestry`() {
    val context = scenarios().single { it.rule == "ScopedConfigurationContextRule" }
    exercise(context.copy(body = context.body.replace(WORKSPACE.toString(), UUID(0, 99).toString())), 200)
    exercise(context.copy(body = context.body.replace("workspace", "organization").replace(WORKSPACE.toString(), UUID(0, 99).toString())), 200)
    exercise(context.copy(body = context.body.replace("workspace", "actor")), 404)
    val list = scenarios().single { it.rule == "ScopedConfigurationListRule" }
    exercise(list.copy(body = """{"config_key":"unknown-key"}"""), 200)
    exercise(list.copy(body = "{}"), 400)
    exercise(list.copy(body = """{"config_key":"connector_version","origin_type":"USER"}"""), 400)
  }

  @Test
  fun `nondefinition resource fields retain their actual display only behavior`() {
    for (scenario in scenarios().filter { it.create || it.update || it.rule == "ScopedConfigurationReadRule" }) {
      val body =
        scenario.body
          .replace("connector_version", "network_security_token")
          .replace(VERSION.toString(), "opaque-token")
          .replace("actor_definition", "user")
          .replace(DEFINITION.toString(), UUID(0, 99).toString())
      exercise(scenario.copy(body = body), 200, displayResource = true)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    foreignVersion: Boolean = false,
    absent: Boolean = false,
    displayResource: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val configs = mockk<ScopedConfigurationService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(ScopedConfigurationApiController::class.java),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet() + "io.airbyte.domain.services.auth.ScopedConfigurationAuthorizationService",
      security,
    ).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<ScopedConfigurationHandler>().single()
        val read =
          ScopedConfigurationRead(
            id = CONFIG.toString(),
            configKey = "connector_version",
            value = VERSION.toString(),
            scopeType = "workspace",
            scopeId = WORKSPACE.toString(),
            scopeName = "fixture",
            resourceType = "actor_definition",
            resourceId = DEFINITION.toString(),
            resourceName = "fixture",
            originType = "user",
            origin = USER.toString(),
          )
        val resource = if (displayResource) UUID(0, 99) else DEFINITION
        every { handler.insertScopedConfiguration(match { it.scopeId == WORKSPACE.toString() && it.resourceId == resource.toString() }) } returns read
        every {
          handler.updateScopedConfiguration(
            CONFIG,
            match { it.scopeId == WORKSPACE.toString() && it.resourceId == resource.toString() },
          )
        } returns read
        every { handler.getScopedConfiguration(CONFIG) } returns read
        every { handler.deleteScopedConfiguration(CONFIG) } just Runs
        every { handler.listScopedConfigurations("connector_version") } returns listOf(read)
        every { handler.listScopedConfigurations("unknown-key") } returns emptyList()
        every { handler.getScopedConfigurationContext(match { it.scopeId == WORKSPACE && it.resourceId == DEFINITION }) } returns
          ScopedConfigurationContextResponse(activeConfiguration = read, ancestorConfigurations = emptyList(), descendantConfigurations = emptyList())
        every {
          handler.getScopedConfigurationContext(
            match {
              it.scopeId ==
                UUID(
                  0,
                  99,
                ) && it.resourceId == DEFINITION
            },
          )
        } returns
          ScopedConfigurationContextResponse(ancestorConfigurations = emptyList(), descendantConfigurations = emptyList())
        every { configs.getScopedConfigurationMetadata(CONFIG) } returns
          ScopedConfigurationMetadata(
            CONFIG,
            if (displayResource) "network_security_token" else "connector_version",
            ConfigScopeType.WORKSPACE,
            WORKSPACE,
            Optional.of(if (displayResource) ConfigResourceType.USER else ConfigResourceType.ACTOR_DEFINITION),
            Optional.of(resource),
            ConfigOriginType.USER,
            USER.toString(),
          )
        every { configs.getConnectorVersionSelector(CONFIG) } returns VERSION.toString()
        every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.requireUser(USER) } just Runs
        every { data.requireSavedActorDefinition(DEFINITION) } just Runs
        every { data.requireSavedActorDefinition(UUID(0, 99)) } throws AuthorizationResourceNotFoundException()
        every { data.savedDefinitionVersionParent(VERSION) } returns if (foreignVersion) UUID(0, 99) else DEFINITION
        every { data.existingSavedActorWorkspaceAndOrganization(WORKSPACE) } returns Optional.empty()
        if (absent) {
          every { configs.getScopedConfigurationMetadata(CONFIG) } throws
            ConfigNotFoundException(
              ConfigNotFoundType.SCOPED_CONFIGURATION,
              CONFIG,
            )
        }
        if (missing) every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } throws AuthorizationResourceNotFoundException()
        if (lookupError) {
          every {
            data.savedWorkspaceAndOrganization(
              WORKSPACE,
              true,
            )
          } throws java.io.IOException("scoped configuration workspace unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(ScopedConfigurationService::class.java, configs)
        val definitions = mockk<ActorDefinitionService>()
        every { definitions.getDefaultVersionForActorDefinitionIdOptional(DEFINITION) } returns Optional.empty()
        context.registerSingleton(ActorDefinitionService::class.java, definitions)
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
        assertEquals(
          expected,
          response.statusCode(),
          "${server.url}${scenario.path}: HTTP ${response.version()} ${response.headers().map()} ${response.body()}",
        )
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
        verify(exactly = 0) { configs.getScopedConfiguration(any()) }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val references: Boolean = false,
    val saved: Boolean = false,
    val create: Boolean = false,
    val update: Boolean = false,
  )

  private fun scenarios(): List<Scenario> {
    val create =
      """{"config_key":"connector_version","value":"$VERSION","scope_type":"workspace",""" +
        """"scope_id":"$WORKSPACE","resource_type":"actor_definition","resource_id":"$DEFINITION",""" +
        """"origin_type":"user","origin":"$USER"}"""
    val id = """{"scopedConfigurationId":"$CONFIG"}"""
    return listOf(
      Scenario("/api/v1/scoped_configuration/create", create, "ScopedConfigurationCreateRule", references = true, create = true),
      Scenario(
        "/api/v1/scoped_configuration/update",
        """{"scopedConfigurationId":"$CONFIG","data":$create}""",
        "ScopedConfigurationUpdateRule",
        references = true,
        saved = true,
        update = true,
      ),
      Scenario("/api/v1/scoped_configuration/get", id, "ScopedConfigurationReadRule", saved = true),
      Scenario("/api/v1/scoped_configuration/delete", id, "ScopedConfigurationDeleteRule", saved = true),
      Scenario("/api/v1/scoped_configuration/list", """{"config_key":"connector_version"}""", "ScopedConfigurationListRule"),
      Scenario(
        "/api/v1/scoped_configuration/get_context",
        """{"config_key":"connector_version","scope_type":"workspace","scope_id":"$WORKSPACE","resource_type":"actor_definition","resource_id":"$DEFINITION"}""",
        "ScopedConfigurationContextRule",
      ),
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val CONFIG: UUID = UUID(0, 3)
    val DEFINITION: UUID = UUID(0, 4)
    val VERSION: UUID = UUID(0, 5)
    val USER: UUID = UUID(0, 6)
  }
}
