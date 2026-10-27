/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.BuilderProjectForDefinitionResponse
import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.ConnectorBuilderCapabilities
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectDetailsRead
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectRead
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectReadList
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamRead
import io.airbyte.api.server.generated.models.ConnectorBuilderResolvedManifest
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.SourceDefinitionIdBody
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.shared.ConnectorBuilderProjectMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.ConnectorBuilderProjectApiController
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

abstract class RemainingBuilderAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `builder operations reject unsupported callers before testing credentials manifests OAuth or project writes`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `builder project and supplied workspace must match even for an administrator with access to both`() {
    assertAll(
      scenarios().filter { it.saved && it.suffix != "get_for_definition_id" }.map { scenario ->
        Executable { exercise(scenario, 404, foreignProject = true, grant = Permission.PermissionType.INSTANCE_ADMIN, declaration = false) }
      },
    )
  }

  @Test
  fun `builder base version references require source kind and accessible saved definition parents before writes`() {
    assertAll(
      scenarios().filter { it.base || it.suffix == "fork" }.map { scenario ->
        Executable { exercise(scenario, 404, foreignVersion = true, declaration = false) }
      },
    )
  }

  @Test
  fun `simple resolve keeps optional project tracing context while full resolve requires a saved project`() {
    val resolve = scenarios().single { it.suffix == "resolve" }
    exercise(resolve.copy(body = resolve.body.replace(PROJECT.toString(), UUID(0, 99).toString())), 200, declaration = false)
    exercise(resolve.copy(body = """{"workspaceId":"$WORKSPACE","manifest":{}}"""), 200, declaration = false)
    val full = scenarios().single { it.suffix == "full_resolve" }
    exercise(full.copy(body = full.body.replace(PROJECT.toString(), UUID(0, 99).toString())), 404)
  }

  @Test
  fun `fifteen builder operations declare exact read write and semantic reference policies and execute normal output`() {
    for (scenario in scenarios()) {
      exercise(scenario, scenario.success)
      exercise(scenario, 403, granted = false)
      exercise(scenario, if (scenario.write) 403 else scenario.success, grant = Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR)
      exercise(scenario, 403, token = "embedded-scoped:$WORKSPACE", granted = false)
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, scenario.success, token = "scoped:$WORKSPACE")
      exercise(scenario, scenario.success, token = "internal")
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, scenario.success, granted = false, security = false)
      if (scenario.saved) exercise(scenario, 404, missing = true)
      if (scenario.write) exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
    }
  }

  @Test
  fun `builder patch fallback and versioned reads validate only actually consumed base metadata`() {
    val update = scenarios().single { it.suffix == "update" }
    exercise(update, 204, poisonBase = true)
    exercise(update.copy(body = update.body.replace(",\"baseActorDefinitionVersionId\":\"$VERSION\"", "")), 404, poisonBase = true)
    val get = scenarios().single { it.suffix == "get_with_manifest" }
    exercise(get, 404, poisonBase = true)
    exercise(get.copy(body = """{"workspaceId":"$WORKSPACE","builderProjectId":"$PROJECT","version":1}"""), 200, poisonBase = true)
    for (scenario in scenarios().filter { it.suffix in setOf("delete", "publish", "read_stream", "full_resolve", "update_testing_values") }) {
      exercise(scenario, scenario.success, poisonBase = true, usable = false)
    }
  }

  @Test
  fun `builder list tolerates inaccessible base versions and definition lookup supports another workspace in the organization`() {
    val list = scenarios().single { it.suffix == "list" }
    exercise(list, 200, foreignListSecond = true)
    exercise(list, 200, poisonListSecondBase = true)
    val definition = scenarios().single { it.suffix == "get_for_definition_id" }
    exercise(definition.copy(body = definition.body.replace(DEFINITION.toString(), UUID(0, 99).toString())), 200, absentDefinitionProject = true)
    exercise(definition, 200, foreignProject = true)
    exercise(definition, 404, wrongProjectDefinition = true)
    for (scenario in scenarios().filter { it.rule == "BuilderOAuthRule" }) {
      exercise(scenario, 403, hasDraft = false, usable = false)
      exercise(scenario, 200, hasDraft = true, usable = false)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.write) {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.WORKSPACE_READER
      },
    granted: Boolean = true,
    foreignProject: Boolean = false,
    foreignVersion: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    poisonBase: Boolean = false,
    hasDraft: Boolean = true,
    usable: Boolean = true,
    foreignListSecond: Boolean = false,
    poisonListSecondBase: Boolean = false,
    absentDefinitionProject: Boolean = false,
    wrongProjectDefinition: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val projects = mockk<ConnectorBuilderService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(ConnectorBuilderProjectApiController::class.java),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet() + "io.airbyte.domain.services.auth.BuilderAuthorizationDataService",
      security,
    ).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<ConnectorBuilderProjectsHandler>().single()
        val details = ConnectorBuilderProjectDetailsRead("fixture", PROJECT, true)
        val json = Jsons.jsonNode(mapOf("fixture" to "value"))
        every { handler.createConnectorBuilderProject(match { it.workspaceId == WORKSPACE }) } returns
          ConnectorBuilderProjectIdWithWorkspaceId(WORKSPACE, PROJECT)
        every { handler.createForkedConnectorBuilderProject(match { it.workspaceId == WORKSPACE && it.baseActorDefinitionId == DEFINITION }) } returns
          ConnectorBuilderProjectIdWithWorkspaceId(WORKSPACE, PROJECT)
        every { handler.deleteConnectorBuilderProject(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } just Runs
        every { handler.getConnectorBuilderProjectWithManifest(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          ConnectorBuilderProjectRead(details, testingValues = json)
        every { handler.listConnectorBuilderProjects(match { it.workspaceId == WORKSPACE }) } returns
          ConnectorBuilderProjectReadList(listOf(details, details.copy(builderProjectId = SECOND_PROJECT)))
        every { handler.publishConnectorBuilderProject(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          SourceDefinitionIdBody(DEFINITION)
        every { handler.readConnectorBuilderProjectStream(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          ConnectorBuilderProjectStreamRead(emptyList(), emptyList(), false)
        every { handler.fullResolveManifestBuilderProject(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          ConnectorBuilderResolvedManifest(json)
        every { handler.resolveManifest(any(), WORKSPACE, PROJECT) } returns json
        every { handler.resolveManifest(any(), WORKSPACE, UUID(0, 99)) } returns json
        every { handler.resolveManifest(any(), WORKSPACE, null) } returns json
        every { handler.getCapabilities() } returns ConnectorBuilderCapabilities(true)
        every { handler.updateConnectorBuilderProject(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } just Runs
        every { handler.updateConnectorBuilderProjectTestingValues(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          json
        val queriedDefinition = if (absentDefinitionProject) UUID(0, 99) else DEFINITION
        every {
          handler.getConnectorBuilderProjectForDefinitionId(match { it.workspaceId == WORKSPACE && it.actorDefinitionId == queriedDefinition })
        } returns
          if (absentDefinitionProject) BuilderProjectForDefinitionResponse(null) else BuilderProjectForDefinitionResponse(PROJECT, WORKSPACE)
        every { handler.getConnectorBuilderProjectOAuthConsent(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          OAuthConsentRead("https://example.com/consent")
        every { handler.completeConnectorBuilderProjectOAuth(match { it.workspaceId == WORKSPACE && it.builderProjectId == PROJECT }) } returns
          CompleteOAuthResponse(true, mapOf("fixture" to "credential"))
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        val project =
          ConnectorBuilderProjectMetadata(
            PROJECT,
            Optional.of(if (foreignProject) UUID(0, 99) else WORKSPACE),
            Optional.of(if (wrongProjectDefinition) UUID(0, 99) else DEFINITION),
            Optional.of(if (poisonBase) UUID(0, 99) else VERSION),
            hasDraft,
          )
        every { projects.getConnectorBuilderProjectMetadata(PROJECT) } returns project
        every { projects.getConnectorBuilderProjectMetadata(UUID(0, 99)) } throws
          ConfigNotFoundException(ConfigNotFoundType.CONNECTOR_BUILDER_PROJECT, UUID(0, 99))
        every { projects.listConnectorBuilderProjectMetadata(WORKSPACE) } returns
          listOf(
            project,
            project.copy(
              id = SECOND_PROJECT,
              workspace = Optional.of(if (foreignListSecond) UUID(0, 99) else WORKSPACE),
              baseVersion = Optional.of(if (poisonListSecondBase) UUID(0, 99) else VERSION),
            ),
          )
        every { projects.getConnectorBuilderProjectIdForActorDefinitionId(queriedDefinition) } returns
          if (absentDefinitionProject) Optional.empty() else Optional.of(PROJECT)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.workspaceAndOrganization(UUID(0, 99)) } returns WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
        every { data.savedDefinitionVersionParent(VERSION) } returns if (foreignVersion) UUID(0, 99) else DEFINITION
        every { data.savedDefinitionVersionParent(UUID(0, 99)) } throws AuthorizationResourceNotFoundException()
        every { data.requireActorDefinitionType(DEFINITION, true) } just Runs
        every { data.requireDefinitionDefaultVersion(DEFINITION) } just Runs
        every { data.requireActorDefinitionType(UUID(0, 99), true) } throws AuthorizationResourceNotFoundException()
        every { data.canUseActorDefinition(DEFINITION, owner) } returns usable
        if (foreignVersion &&
          scenario.suffix == "fork"
        ) {
          every { data.requireActorDefinitionType(DEFINITION, true) } throws AuthorizationResourceNotFoundException()
        }
        if (missing) {
          every { projects.getConnectorBuilderProjectMetadata(PROJECT) } throws
            ConfigNotFoundException(ConfigNotFoundType.CONNECTOR_BUILDER_PROJECT, PROJECT)
        }
        if (lookupError) every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("builder workspace metadata unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.workspaceId = WORKSPACE
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(ConnectorBuilderService::class.java, projects)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (absentDefinitionProject && expected == 200) assertEquals("{}", response.body())
        if (expected != scenario.success) RemainingOperationHttpSupport.requireNoBusiness(business)
        verify(exactly = 0) { projects.getConnectorBuilderProject(any(), any()) }
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val body: String,
    val rule: String,
    val success: Int = 200,
    val write: Boolean = false,
    val saved: Boolean = false,
    val base: Boolean = false,
  ) {
    val path = "/api/v1/connector_builder_projects/$suffix"
  }

  private fun scenarios(): List<Scenario> {
    val workspace = "\"workspaceId\":\"$WORKSPACE\""
    val target = "$workspace,\"builderProjectId\":\"$PROJECT\""
    val details = """{"name":"fixture","draftManifest":{},"baseActorDefinitionVersionId":"$VERSION","contributionActorDefinitionId":"${UUID(
      0,
      999,
    )}"}"""
    return listOf(
      Scenario("create", """{$workspace,"builderProject":$details}""", "BuilderCreateRule", 201, write = true, base = true),
      Scenario("fork", """{$workspace,"baseActorDefinitionId":"$DEFINITION"}""", "BuilderForkRule", 201, write = true),
      Scenario("delete", """{$target}""", "BuilderDeleteRule", 204, write = true, saved = true),
      Scenario("get_with_manifest", """{$target}""", "BuilderSavedReadRule", saved = true),
      Scenario("list", """{$workspace}""", "BuilderProjectListRule"),
      Scenario(
        "publish",
        """{$target,"name":"fixture","initialDeclarativeManifest":{"description":"fixture","manifest":{},"spec":{},"version":1}}""",
        "BuilderPublishRule",
        write = true,
        saved = true,
      ),
      Scenario("read_stream", """{$target,"manifest":{},"streamName":"fixture"}""", "BuilderTestingReadRule", saved = true),
      Scenario("full_resolve", """{$target,"manifest":{}}""", "BuilderTestingReadRule", saved = true),
      Scenario("resolve", """{$target,"manifest":{}}""", "BuilderResolveRule"),
      Scenario("capabilities", """{$workspace}""", "BuilderCapabilitiesRule"),
      Scenario("update", """{$target,"builderProject":$details}""", "BuilderUpdateRule", 204, write = true, saved = true, base = true),
      Scenario("update_testing_values", """{$target,"spec":{},"testingValues":{}}""", "BuilderTestingWriteRule", write = true, saved = true),
      Scenario("get_for_definition_id", """{$workspace,"actorDefinitionId":"$DEFINITION"}""", "BuilderDefinitionProjectRule", saved = true),
      Scenario("get_oauth_consent_url", """{$target,"redirectUrl":"https://example.com/redirect"}""", "BuilderOAuthRule", saved = true),
      Scenario("complete_oauth", """{$target,"redirectUrl":"https://example.com/redirect","queryParams":{}}""", "BuilderOAuthRule", saved = true),
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val PROJECT: UUID = UUID(0, 3)
    val DEFINITION: UUID = UUID(0, 4)
    val VERSION: UUID = UUID(0, 5)
    val SECOND_PROJECT: UUID = UUID(0, 6)
  }
}
