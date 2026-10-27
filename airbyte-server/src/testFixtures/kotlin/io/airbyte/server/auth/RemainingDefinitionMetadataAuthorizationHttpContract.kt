/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionResponse
import io.airbyte.api.server.generated.models.SourceDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.SupportState
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.ActorDefinitionVersionApiController
import io.airbyte.server.apis.controllers.DestinationDefinitionSpecificationApiController
import io.airbyte.server.apis.controllers.SourceDefinitionSpecificationApiController
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

abstract class RemainingDefinitionMetadataAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `definition versions and specifications reject unsupported credentials before metadata business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `all eight metadata operations execute their exact declared policy and normal typed responses`() {
    for (scenario in scenarios()) exercise(scenario, 200)
  }

  @Test
  fun `authentication resource metadata stays grant independent while explicit global operations require their action`() {
    for (scenario in scenarios()) {
      exercise(scenario, if (scenario.global) 403 else 200, granted = false)
      if (scenario.global) {
        exercise(scenario, 403, grant = Permission.PermissionType.ORGANIZATION_ADMIN)
      } else {
        exercise(scenario, 200, token = "scoped:$WORKSPACE", granted = false)
        exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      }
      exercise(scenario, 200, granted = false, security = false)
    }
    val resolve = scenarios().single { it.path.endsWith("/resolve") }
    exercise(resolve, 200, token = "service", grant = Permission.PermissionType.DATAPLANE)
    exercise(resolve, 403, token = "service", granted = false)
  }

  @Test
  fun `typed saved actor and definition failures never become global metadata fallback`() {
    for (scenario in scenarios().filter { !it.global }) {
      exercise(scenario, 404, missing = true, declaration = false)
      exercise(scenario, 500, lookupError = true, declaration = false)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.INSTANCE_ADMIN,
    granted: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val controllers =
      listOf(
        ActorDefinitionVersionApiController::class.java,
        if (cloud) {
          Class.forName(
            "io.airbyte.server.wrapped.apis.SourceDefinitionSpecificationApiWrappedController",
          )
        } else {
          SourceDefinitionSpecificationApiController::class.java
        },
        if (cloud) {
          Class.forName(
            "io.airbyte.server.wrapped.apis.DestinationDefinitionSpecificationApiWrappedController",
          )
        } else {
          DestinationDefinitionSpecificationApiController::class.java
        },
      )
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(), security).run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is ActorDefinitionVersionHandler -> {
              every { instance.getActorDefinitionVersionForSourceId(match { it.sourceId == ACTOR }) } returns version()
              every { instance.getActorDefinitionVersionForDestinationId(match { it.destinationId == ACTOR }) } returns version()
              every { instance.getDefaultVersion(match { it.actorDefinitionId == DEFINITION }) } returns version()
              every {
                instance.resolveActorDefinitionVersionByTag(
                  match { it.actorDefinitionId == DEFINITION && it.dockerImageTag == "1.0" },
                )
              } returns
                ResolveActorDefinitionVersionResponse(VERSION, "fixture", "1.0", false, false, false)
            }
            is ConnectorDefinitionSpecificationHandler -> {
              every { instance.getSpecificationForSourceId(match { it.sourceId == ACTOR }) } returns sourceSpecification()
              every { instance.getSpecificationForDestinationId(match { it.destinationId == ACTOR }) } returns destinationSpecification()
              every {
                instance.getSourceDefinitionSpecification(
                  match { it.sourceDefinitionId == DEFINITION && it.workspaceId == WORKSPACE },
                )
              } returns
                sourceSpecification()
              every {
                instance.getDestinationSpecification(
                  match { it.destinationDefinitionId == DEFINITION && it.workspaceId == WORKSPACE },
                )
              } returns
                destinationSpecification()
            }
          }
          configureDependency(instance)
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        val source = scenario.path.contains("source") || scenario.path.endsWith("/resolve")
        if (source) {
          every { data.savedSourceWorkspaceAndOrganization(ACTOR) } returns owner
        } else {
          every { data.savedDestinationWorkspaceAndOrganization(ACTOR) } returns owner
        }
        every { data.savedActorDefinitionId(ACTOR, WORKSPACE) } returns DEFINITION
        every { data.requireActorDefinitionType(DEFINITION, source) } just Runs
        every { data.canUseActorDefinition(DEFINITION, owner) } returns true
        every { data.requireDefinitionDefaultVersion(DEFINITION) } just Runs
        if (missing) every { data.requireActorDefinitionType(DEFINITION, any()) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.requireActorDefinitionType(DEFINITION, any()) } throws java.io.IOException("saved definition unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("ORGANIZATION")) permission.organizationId = ORGANIZATION
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
        if (expected != 200) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        } else {
          assertTrue(
            response.body().contains(if (scenario.path.contains("specifications")) "\"jobInfo\"" else "\"dockerRepository\""),
            response.body(),
          )
        }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val global: Boolean = false,
  )

  private fun scenarios() =
    listOf(
      Scenario("/api/v1/actor_definition_versions/get_for_source", """{"sourceId":"$ACTOR"}""", "DefinitionSourceMetadataRule"),
      Scenario("/api/v1/actor_definition_versions/get_for_destination", """{"destinationId":"$ACTOR"}""", "DefinitionDestinationMetadataRule"),
      Scenario("/api/v1/actor_definition_versions/get_default", """{"actorDefinitionId":"$DEFINITION"}""", "DefinitionDefaultVersionRule", true),
      Scenario(
        "/api/v1/actor_definition_versions/resolve",
        """{"actorDefinitionId":"$DEFINITION","actorType":"source","dockerImageTag":"1.0"}""",
        "DefinitionResolveVersionRule",
        true,
      ),
      Scenario(
        "/api/v1/source_definition_specifications/get",
        """{"sourceDefinitionId":"$DEFINITION","workspaceId":"$WORKSPACE"}""",
        "DefinitionSourceMetadataRule",
      ),
      Scenario("/api/v1/source_definition_specifications/get_for_source", """{"sourceId":"$ACTOR"}""", "DefinitionSourceMetadataRule"),
      Scenario(
        "/api/v1/destination_definition_specifications/get",
        """{"destinationDefinitionId":"$DEFINITION","workspaceId":"$WORKSPACE"}""",
        "DefinitionDestinationMetadataRule",
      ),
      Scenario(
        "/api/v1/destination_definition_specifications/get_for_destination",
        """{"destinationId":"$ACTOR"}""",
        "DefinitionDestinationMetadataRule",
      ),
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
    val ACTOR: UUID = UUID(0, 4)
    val VERSION: UUID = UUID(0, 5)

    fun version() = ActorDefinitionVersionRead("fixture", "1.0", false, false, SupportState.SUPPORTED, false, false)

    fun job() = SynchronousJobRead(VERSION, JobConfigType.GET_SPEC, 0L, 0L, true)

    fun sourceSpecification() = SourceDefinitionSpecificationRead(DEFINITION, job())

    fun destinationSpecification() = DestinationDefinitionSpecificationRead(DEFINITION, job())
  }
}
