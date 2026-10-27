/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.DefinitionAccessService
import io.airbyte.server.apis.controllers.DestinationDefinitionApiController
import io.airbyte.server.apis.controllers.SourceDefinitionApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingDefinitionMutationAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `remaining custom definition mutations reject unsupported credentials before validation SPEC or persistence`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `definition updates require the consumed saved SPEC workspace and definition availability before business`() {
    assertAll(scenarios().filter { it.update }.map { scenario -> Executable { exercise(scenario, 404, usable = false, declaration = false) } })
  }

  @Test
  fun `all six mutation routes execute independent edition policies and normal responses`() {
    for (scenario in scenarios()) exercise(scenario, scenario.status)
  }

  @Test
  fun `custom uploads keep source editor intent for both kinds and effective legacy workspace precedence`() {
    for (scenario in scenarios().filter { it.create }) {
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR)
      exercise(scenario.copy(body = scenario.body.dropLast(1) + ",\"scopeId\":\"${UUID(0, 99)}\",\"scopeType\":\"organization\"}"), 200)
      val scoped = scenario.copy(body = scenario.body.replace("\"workspaceId\"", "\"scopeId\"").dropLast(1) + ",\"scopeType\":\"workspace\"}")
      exercise(scoped, 200)
    }
  }

  @Test
  fun `organization only custom uploads retain unsupported errors and do not consume an invented default workspace`() {
    for (scenario in scenarios().filter { it.create }) {
      val organization =
        scenario.copy(
          body =
            scenario.body.replace(
              "\"workspaceId\":\"$WORKSPACE\"",
              "\"scopeId\":\"$ORGANIZATION\",\"scopeType\":\"organization\"",
            ),
        )
      exercise(organization, 422, grant = Permission.PermissionType.ORGANIZATION_EDITOR)
      exercise(organization, 403, grant = Permission.PermissionType.ORGANIZATION_READER)
      exercise(organization, 422, granted = false, security = false)
    }
  }

  @Test
  fun `definition mutation edition predicates keep authentication access versus actual admin or any custom workspace access`() {
    for (scenario in scenarios().filter { !it.create }) {
      exercise(scenario, scenario.status, granted = false)
      exercise(scenario, if (cloud) 422 else scenario.status, granted = false, customAccess = false)
      exercise(scenario, scenario.status, customAccess = false, identityError = true)
      exercise(scenario, if (cloud) 401 else scenario.status, granted = false, identityError = true)
      exercise(scenario, if (cloud) 500 else scenario.status, granted = false, customLookupError = true)
      exercise(scenario, scenario.status, granted = false, customAccess = false, identityError = true, security = false)
    }
  }

  @Test
  fun `definition mutation caller and required SPEC scope checks remain explicit after an earlier allow`() {
    for (scenario in scenarios()) {
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, scenario.status, token = "internal")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      if (scenario.create || scenario.update) {
        exercise(scenario, scenario.status, token = "scoped:$WORKSPACE")
        exercise(scenario, 404, missing = true)
        exercise(scenario, 500, lookupError = true)
      }
      exercise(scenario, scenario.status, granted = false, security = false)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.create) {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.INSTANCE_ADMIN
      },
    granted: Boolean = true,
    usable: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    customAccess: Boolean = true,
    identityError: Boolean = false,
    customLookupError: Boolean = false,
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
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { it.rule }.toSet(), security).run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is SourceDefinitionsHandler -> {
              every { instance.createCustomSourceDefinition(any()) } returns RemainingDefinitionRegistryAuthorizationHttpContract.source()
              every { instance.updateSourceDefinition(match { it.sourceDefinitionId == DEFINITION && it.workspaceId == WORKSPACE }) } returns
                RemainingDefinitionRegistryAuthorizationHttpContract.source()
              every { instance.deleteSourceDefinition(DEFINITION) } just Runs
            }
            is DestinationDefinitionsHandler -> {
              every { instance.createCustomDestinationDefinition(any()) } returns RemainingDefinitionRegistryAuthorizationHttpContract.destination()
              every {
                instance.updateDestinationDefinition(
                  match { it.destinationDefinitionId == DEFINITION && it.workspaceId == WORKSPACE },
                )
              } returns
                RemainingDefinitionRegistryAuthorizationHttpContract.destination()
              every { instance.deleteDestinationDefinition(DEFINITION) } just Runs
            }
            is ActorDefinitionAccessValidator -> every { instance.validateWriteAccess(DEFINITION) } just Runs
          }
          configureDependency(instance)
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        val source = scenario.path.contains("source_definitions")
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.requireActorDefinitionType(DEFINITION, source) } just Runs
        every { data.requireDefinitionDefaultVersion(DEFINITION) } just Runs
        every { data.canUseActorDefinition(DEFINITION, owner) } returns usable
        if (missing) every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("SPEC workspace unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("WORKSPACE")) permission.workspaceId = WORKSPACE
        if (grant.name.startsWith("ORGANIZATION")) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        val access = mockk<DefinitionAccessService>()
        every { access.currentUserId("caller") } returns UUID(0, 7)
        every { access.hasAccessibleCustomDefinition(UUID(0, 7), DEFINITION) } returns customAccess
        if (identityError) every { access.currentUserId("caller") } throws NoSuchElementException("current user missing")
        if (customLookupError) {
          every {
            access.hasAccessibleCustomDefinition(
              UUID(
                0,
                7,
              ),
              DEFINITION,
            )
          } throws java.io.IOException("custom definition grants unavailable")
        }
        context.registerSingleton(DefinitionAccessService::class.java, access)
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
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.headers().map()} ${response.body()}")
        if (expected !in setOf(200, 204)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (!security) verify(exactly = 0) { context.getBean(DefinitionAccessService::class.java).currentUserId(any()) }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val create: Boolean = false,
    val update: Boolean = false,
    val status: Int = 200,
  )

  private fun scenarios(): List<Scenario> {
    val source =
      """{"workspaceId":"$WORKSPACE","sourceDefinition":{"name":"fixture","dockerRepository":"fixture",""" +
        """"dockerImageTag":"1.0","documentationUrl":"https://example.com/fixture"}}"""
    val destination = source.replace("sourceDefinition", "destinationDefinition")
    return listOf(
      Scenario("/api/v1/source_definitions/create_custom", source, "io.airbyte.server.auth.CustomDefinitionCreateRule", create = true),
      Scenario("/api/v1/destination_definitions/create_custom", destination, "io.airbyte.server.auth.CustomDefinitionCreateRule", create = true),
      Scenario(
        "/api/v1/source_definitions/update",
        """{"sourceDefinitionId":"$DEFINITION","workspaceId":"$WORKSPACE","dockerImageTag":"1.0"}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.CloudSourceDefinitionMutationRule"
        } else {
          "io.airbyte.server.auth.SourceDefinitionMutationRule"
        },
        update = true,
      ),
      Scenario(
        "/api/v1/destination_definitions/update",
        """{"destinationDefinitionId":"$DEFINITION","workspaceId":"$WORKSPACE","dockerImageTag":"1.0"}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.CloudDestinationDefinitionMutationRule"
        } else {
          "io.airbyte.server.auth.DestinationDefinitionMutationRule"
        },
        update = true,
      ),
      Scenario(
        "/api/v1/source_definitions/delete",
        """{"sourceDefinitionId":"$DEFINITION"}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.CloudSourceDefinitionMutationRule"
        } else {
          "io.airbyte.server.auth.SourceDefinitionMutationRule"
        },
        status = 204,
      ),
      Scenario(
        "/api/v1/destination_definitions/delete",
        """{"destinationDefinitionId":"$DEFINITION"}""",
        if (cloud) {
          "io.airbyte.server.wrapped.auth.CloudDestinationDefinitionMutationRule"
        } else {
          "io.airbyte.server.auth.DestinationDefinitionMutationRule"
        },
        status = 204,
      ),
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
  }
}
