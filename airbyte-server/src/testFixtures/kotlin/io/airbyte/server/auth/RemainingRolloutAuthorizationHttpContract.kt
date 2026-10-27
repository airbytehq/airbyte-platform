/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.server.generated.models.ConnectorRolloutManualFinalizeResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutManualRolloutResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutRead
import io.airbyte.api.server.generated.models.ConnectorRolloutState
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectorRolloutHandler
import io.airbyte.commons.server.handlers.ConnectorRolloutHandlerManual
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.ConnectorRolloutApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

abstract class RemainingRolloutAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `connector rollout endpoints reject unsupported callers before workflow pin or metadata business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `rollout workflow writes reject human and application owner admin identities while manual routes remain available`() {
    for (scenario in scenarios().filter { it.worker }) {
      exercise(scenario, 403, token = "user", declaration = false)
      exercise(scenario, 403, token = "application-owner", declaration = false)
    }
  }

  @Test
  fun `rollout actor pin selection checks every selected actor definition before any pin write or workflow call`() {
    for (scenario in scenarios().filter { it.actors }) exercise(scenario, 400, foreignActor = true, declaration = false)
  }

  @Test
  fun `all thirteen rollout operations declare independent worker manual admin and filter policies with normal responses`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 403, token = "service", granted = false)
      exercise(scenario, 403, token = "user", grant = Permission.PermissionType.ORGANIZATION_ADMIN)
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "scoped:$WORKSPACE")
      exercise(scenario, 200, token = "user", granted = false, security = false)
      if (!scenario.filter) {
        exercise(scenario, if (scenario.start) 400 else 404, missing = true)
        exercise(scenario, 500, lookupError = true)
      }
    }
  }

  @Test
  fun `rollout ID precedence fallback filters and manual candidate relations match actual consumed selectors`() {
    val update = scenarios().single { it.suffix == "update_state" }
    exercise(update.copy(body = """{"id":"$ROLLOUT","state":"paused","actor_definition_id":"${UUID(0, 99)}","docker_image_tag":"ignored"}"""), 200)
    val fallback = update.copy(body = """{"state":"paused","actor_definition_id":"$DEFINITION","docker_image_tag":"1.0"}""")
    exercise(fallback, 200)
    exercise(fallback, 400, missing = true)
    exercise(fallback, 400, rolloutCount = 0)
    exercise(fallback, 400, rolloutCount = 2)
    for (scenario in scenarios().filter { it.rule == "RolloutManualChangeRule" }) {
      exercise(scenario.copy(body = scenario.body.replace("fixture/repo", "foreign/repo")), 400)
    }
    for (scenario in scenarios().filter { it.actors }) {
      exercise(scenario.copy(body = scenario.body.replace("\"$ACTOR\",\"$SECOND_ACTOR\"", "")), 200)
    }
    for (scenario in scenarios().filter { it.rule == "RolloutFilterRule" }) {
      exercise(scenario.copy(body = scenario.body.replace(DEFINITION.toString(), UUID(0, 99).toString())), 200, unknownFilter = true)
    }
    for (scenario in scenarios().filter { it.worker }) {
      exercise(scenario, 200, token = "service")
      exercise(scenario, 200, token = "dataplane")
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = if (scenario.worker) "internal" else "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    granted: Boolean = true,
    foreignActor: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    rolloutCount: Int = 1,
    unknownFilter: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val rollouts = mockk<ConnectorRolloutService>()
    val definitions = mockk<ActorDefinitionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(ConnectorRolloutApiController::class.java),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet() + "io.airbyte.domain.services.auth.RolloutAuthorizationDataService",
      security,
    ).run(
      configure = { context, business ->
        val read = ConnectorRolloutRead(ROLLOUT, DEFINITION, VERSION, ConnectorRolloutState.IN_PROGRESS)
        for (instance in business) {
          when (instance) {
            is ConnectorRolloutHandler -> {
              every { instance.startConnectorRollout(match { it.id == ROLLOUT }) } returns read
              every { instance.doConnectorRollout(match { it.id == ROLLOUT }) } returns read
              every { instance.finalizeConnectorRollout(match { it.id == ROLLOUT }) } returns read
              every { instance.getConnectorRollout(ROLLOUT) } returns read
              every { instance.updateState(any()) } returns read
            }
            is ConnectorRolloutHandlerManual -> {
              every { instance.manualStartConnectorRollout(match { it.actorDefinitionId == DEFINITION }) } returns read
              every { instance.manualDoConnectorRollout(match { it.id == ROLLOUT && it.actorDefinitionId == DEFINITION }) } returns
                ConnectorRolloutManualRolloutResponse(status = "ok")
              every { instance.manualFinalizeConnectorRollout(match { it.id == ROLLOUT && it.actorDefinitionId == DEFINITION }) } returns
                ConnectorRolloutManualFinalizeResponse(status = "ok")
              every { instance.manualPauseConnectorRollout(match { it.id == ROLLOUT }) } returns read
              every { instance.listConnectorRollouts() } returns listOf(read)
              every { instance.listConnectorRollouts(DEFINITION) } returns listOf(read)
              every { instance.listConnectorRollouts(DEFINITION, "1.0") } returns listOf(read)
              every { instance.listConnectorRollouts(UUID(0, 99)) } returns emptyList()
              every { instance.listConnectorRollouts(UUID(0, 99), "1.0") } returns emptyList()
              every { instance.getActorSelectionInfoForPinnedActors(ROLLOUT) } returns
                ConnectorRolloutActorSelectionInfo(0, 0, 0)
            }
            is ConnectorRolloutHelper -> every { instance.getActorSyncInfo(ROLLOUT) } returns emptyMap()
          }
        }
        val saved =
          ConnectorRollout(
            ROLLOUT,
            actorDefinitionId = DEFINITION,
            releaseCandidateVersionId = VERSION,
            initialVersionId = INITIAL,
            state = ConnectorEnumRolloutState.INITIALIZED,
            hasBreakingChanges = false,
            createdAt = 0,
            updatedAt = 0,
            tag = null,
          )
        every { rollouts.getConnectorRollout(ROLLOUT) } returns saved
        every { rollouts.listConnectorRollouts(DEFINITION, VERSION) } returns List(rolloutCount) { saved }
        every { definitions.getActorDefinitionVersion(VERSION) } returns version(VERSION)
        every { definitions.getActorDefinitionVersion(INITIAL) } returns version(INITIAL)
        every { definitions.getActorDefinitionVersion(DEFINITION, "1.0") } returns Optional.of(version(VERSION))
        every { definitions.getDefaultVersionForActorDefinitionIdOptional(DEFINITION) } returns Optional.of(version(INITIAL))
        every { data.requireSavedActorDefinition(DEFINITION) } just Runs
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.actorWorkspaceAndOrganization(ACTOR) } returns owner
        every { data.actorWorkspaceAndOrganization(SECOND_ACTOR) } returns owner
        every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } returns owner
        every { data.savedActorDefinitionId(ACTOR, WORKSPACE) } returns DEFINITION
        every { data.savedActorDefinitionId(SECOND_ACTOR, WORKSPACE) } returns if (foreignActor) UUID(0, 99) else DEFINITION
        if (missing) {
          every { rollouts.getConnectorRollout(ROLLOUT) } throws ConfigNotFoundException("ConnectorRollout", ROLLOUT.toString())
          every { definitions.getActorDefinitionVersion(DEFINITION, "1.0") } returns Optional.empty()
        }
        if (lookupError) {
          every { rollouts.getConnectorRollout(ROLLOUT) } throws java.io.IOException("rollout metadata unavailable")
          every { definitions.getActorDefinitionVersion(DEFINITION, "1.0") } throws java.io.IOException("candidate metadata unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(ConnectorRolloutService::class.java, rollouts)
        context.registerSingleton(ActorDefinitionService::class.java, definitions)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.headers().map()} ${response.body()}")
        if (expected == 200) {
          val bodyFragment =
            when {
              unknownFilter -> "\"connectorRollouts\":[]"
              scenario.suffix == "get_actor_sync_info" -> "\"syncs\":{}"
              scenario.suffix in setOf("manual_rollout", "manual_finalize") -> "\"status\":\"ok\""
              else -> ROLLOUT.toString()
            }
          assertTrue(response.body().contains(bodyFragment), response.body())
        }
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val body: String,
    val rule: String,
    val worker: Boolean = false,
    val filter: Boolean = false,
    val start: Boolean = false,
    val actors: Boolean = false,
  ) {
    val path = "/api/v1/connector_rollout/$suffix"
  }

  private fun scenarios(): List<Scenario> {
    val id = """{"id":"$ROLLOUT"}"""
    val manual = """"docker_repository":"fixture/repo","docker_image_tag":"1.0","actor_definition_id":"$DEFINITION","updated_by":"${UUID(0, 9)}""""
    val actors = """"actor_ids":["$ACTOR","$SECOND_ACTOR"]"""
    return listOf(
      Scenario("start", """{"id":"$ROLLOUT","workflow_run_id":"fixture","rollout_strategy":"automated"}""", "RolloutWorkerRule", worker = true),
      Scenario("rollout", """{"id":"$ROLLOUT","rollout_strategy":"automated",$actors}""", "RolloutWorkerRule", worker = true, actors = true),
      Scenario("finalize", """{"id":"$ROLLOUT","state":"succeeded","rollout_strategy":"automated"}""", "RolloutWorkerRule", worker = true),
      Scenario("update_state", """{"id":"$ROLLOUT","state":"paused"}""", "RolloutWorkerRule", worker = true),
      Scenario("get", id, "RolloutReadRule"),
      Scenario("get_actor_sync_info", id, "RolloutReadRule"),
      Scenario("list", """{"actor_definition_id":"$DEFINITION","docker_image_tag":"1.0"}""", "RolloutFilterRule", filter = true),
      Scenario("list_by_actor_definition_id", """{"actor_definition_id":"$DEFINITION"}""", "RolloutFilterRule", filter = true),
      Scenario("list_all", "{}", "GlobalAdminRule", filter = true),
      Scenario("manual_start", """{$manual,"rollout_strategy":"manual"}""", "RolloutManualStartRule", start = true),
      Scenario("manual_rollout", """{"id":"$ROLLOUT",$manual,$actors}""", "RolloutManualChangeRule", actors = true),
      Scenario("manual_finalize", """{"id":"$ROLLOUT",$manual,"state":"succeeded"}""", "RolloutManualChangeRule"),
      Scenario("manual_pause", """{"id":"$ROLLOUT",$manual,"state":"paused"}""", "RolloutManualChangeRule"),
    )
  }

  companion object {
    val ROLLOUT: UUID = UUID(0, 1)
    val DEFINITION: UUID = UUID(0, 2)
    val VERSION: UUID = UUID(0, 3)
    val INITIAL: UUID = UUID(0, 4)
    val WORKSPACE: UUID = UUID(0, 5)
    val ORGANIZATION: UUID = UUID(0, 6)
    val ACTOR: UUID = UUID(0, 7)
    val SECOND_ACTOR: UUID = UUID(0, 8)

    fun version(id: UUID): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(id)
        .withActorDefinitionId(DEFINITION)
        .withDockerRepository("fixture/repo")
        .withDockerImageTag("1.0")
  }
}
