/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SourceDefinitionApi
import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.CustomSourceDefinitionCreate
import io.airbyte.api.server.generated.models.EnterpriseConnectorStubsReadList
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionRead
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionReadList
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.SourceDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceDefinitionReadList
import io.airbyte.api.server.generated.models.SourceDefinitionUpdate
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.EnterpriseConnectorStubsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.server.apis.execute
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourceDefinitionApiController(
  private val sourceDefinitionsHandler: SourceDefinitionsHandler,
  private val enterpriseConnectorStubsHandler: EnterpriseConnectorStubsHandler,
  private val accessValidator: ActorDefinitionAccessValidator,
) : SourceDefinitionApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.CustomDefinitionCreateRule::class,
  )
  override fun createCustomSourceDefinition(
    @Body customSourceDefinitionCreate: CustomSourceDefinitionCreate?,
  ): SourceDefinitionRead {
    // legacy calls contain workspace id instead of scope id and scope type
    val request =
      customSourceDefinitionCreate!!.let {
        if (it.workspaceId != null) it.copy(scopeType = ScopeType.WORKSPACE, scopeId = it.workspaceId) else it
      }
    return execute {
      sourceDefinitionsHandler.createCustomSourceDefinition(request)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceDefinitionMutationRule::class,
  )
  override fun deleteSourceDefinition(
    @Body sourceDefinitionIdRequestBody: SourceDefinitionIdRequestBody,
  ) {
    execute<Any?> {
      sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody.sourceDefinitionId)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceRegistryMetadataRule::class,
  )
  override fun getSourceDefinition(
    @Body sourceDefinitionIdRequestBody: SourceDefinitionIdRequestBody,
  ): SourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.getSourceDefinition(
        sourceDefinitionIdRequestBody.sourceDefinitionId,
        true,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceRegistryScopedReadRule::class,
  )
  override fun getSourceDefinitionForScope(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): SourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScope,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceRegistryScopedReadRule::class,
  )
  override fun getSourceDefinitionForWorkspace(
    @Body sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId,
  ): SourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.getSourceDefinitionForWorkspace(
        sourceDefinitionIdWithWorkspaceId,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceRegistryGrantRule::class,
  )
  override fun grantSourceDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): PrivateSourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        actorDefinitionIdWithScope,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.AuthenticatedMetadataRule::class,
  )
  override fun listEnterpriseSourceStubs(): EnterpriseConnectorStubsReadList =
    execute {
      enterpriseConnectorStubsHandler.listEnterpriseSourceStubs()
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionRegistryWorkspaceReadRule::class,
  )
  override fun listEnterpriseSourceStubsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): EnterpriseConnectorStubsReadList =
    execute {
      enterpriseConnectorStubsHandler.listEnterpriseSourceStubsForWorkspace(
        workspaceIdRequestBody!!.workspaceId,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.AuthenticatedMetadataRule::class,
  )
  override fun listLatestSourceDefinitions(): SourceDefinitionReadList = execute { sourceDefinitionsHandler.listLatestSourceDefinitions() }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionRegistryPrivateListRule::class,
  )
  override fun listPrivateSourceDefinitions(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): PrivateSourceDefinitionReadList =
    execute {
      sourceDefinitionsHandler.listPrivateSourceDefinitions(
        workspaceIdRequestBody!!,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.AuthenticatedMetadataRule::class,
  )
  override fun listSourceDefinitions(): SourceDefinitionReadList = execute { sourceDefinitionsHandler.listSourceDefinitions() }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionRegistryWorkspaceReadRule::class,
  )
  override fun listSourceDefinitionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdActorDefinitionRequestBody?,
  ): SourceDefinitionReadList =
    execute {
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(
        workspaceIdRequestBody!!,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceRegistryRevokeRule::class,
  )
  override fun revokeSourceDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ) {
    execute<Any?> {
      sourceDefinitionsHandler.revokeSourceDefinition(actorDefinitionIdWithScope)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SourceDefinitionMutationRule::class,
  )
  override fun updateSourceDefinition(
    @Body sourceDefinitionUpdate: SourceDefinitionUpdate?,
  ): SourceDefinitionRead = execute { sourceDefinitionsHandler.updateSourceDefinition(sourceDefinitionUpdate!!) }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
