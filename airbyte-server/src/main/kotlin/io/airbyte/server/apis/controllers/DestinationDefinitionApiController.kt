/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.DestinationDefinitionApi
import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.CustomDestinationDefinitionCreate
import io.airbyte.api.server.generated.models.DestinationDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionReadList
import io.airbyte.api.server.generated.models.DestinationDefinitionUpdate
import io.airbyte.api.server.generated.models.EnterpriseConnectorStubsReadList
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionRead
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionReadList
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.EnterpriseConnectorStubsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationDefinitionApiController(
  private val destinationDefinitionsHandler: DestinationDefinitionsHandler,
  private val accessValidator: ActorDefinitionAccessValidator,
  private val enterpriseConnectorStubsHandler: EnterpriseConnectorStubsHandler,
) : DestinationDefinitionApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.CustomDefinitionCreateRule::class,
  )
  override fun createCustomDestinationDefinition(
    @Body customDestinationDefinitionCreate: CustomDestinationDefinitionCreate?,
  ): DestinationDefinitionRead {
    // legacy calls contain workspace id instead of scope id and scope type
    val request =
      customDestinationDefinitionCreate!!.let {
        if (it.workspaceId != null) it.copy(scopeType = ScopeType.WORKSPACE, scopeId = it.workspaceId) else it
      }
    return execute {
      destinationDefinitionsHandler.createCustomDestinationDefinition(request)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationDefinitionMutationRule::class,
  )
  override fun deleteDestinationDefinition(
    @Body destinationDefinitionIdRequestBody: DestinationDefinitionIdRequestBody,
  ) {
    execute<Any?> {
      destinationDefinitionsHandler.deleteDestinationDefinition(destinationDefinitionIdRequestBody.destinationDefinitionId)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationRegistryMetadataRule::class,
  )
  override fun getDestinationDefinition(
    @Body destinationDefinitionIdRequestBody: DestinationDefinitionIdRequestBody,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.getDestinationDefinition(
        destinationDefinitionIdRequestBody.destinationDefinitionId,
        true,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationRegistryScopedReadRule::class,
  )
  override fun getDestinationDefinitionForScope(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScope,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationRegistryScopedReadRule::class,
  )
  override fun getDestinationDefinitionForWorkspace(
    @Body destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(
        destinationDefinitionIdWithWorkspaceId,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationRegistryGrantRule::class,
  )
  override fun grantDestinationDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): PrivateDestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
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
  override fun listDestinationDefinitions(): DestinationDefinitionReadList =
    execute(
      Callable {
        destinationDefinitionsHandler.listDestinationDefinitions()
      },
    )

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionRegistryWorkspaceReadRule::class,
  )
  override fun listDestinationDefinitionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdActorDefinitionRequestBody?,
  ): DestinationDefinitionReadList =
    execute {
      destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(
        workspaceIdRequestBody!!,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionRegistryWorkspaceReadRule::class,
  )
  override fun listEnterpriseDestinationStubsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): EnterpriseConnectorStubsReadList =
    execute {
      enterpriseConnectorStubsHandler.listEnterpriseDestinationStubsForWorkspace(
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
  override fun listLatestDestinationDefinitions(): DestinationDefinitionReadList =
    execute(
      Callable {
        destinationDefinitionsHandler.listLatestDestinationDefinitions()
      },
    )

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionRegistryPrivateListRule::class,
  )
  override fun listPrivateDestinationDefinitions(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): PrivateDestinationDefinitionReadList =
    execute {
      destinationDefinitionsHandler.listPrivateDestinationDefinitions(
        workspaceIdRequestBody!!,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationRegistryRevokeRule::class,
  )
  override fun revokeDestinationDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ) {
    execute<Any?> {
      destinationDefinitionsHandler.revokeDestinationDefinition(actorDefinitionIdWithScope)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DestinationDefinitionMutationRule::class,
  )
  override fun updateDestinationDefinition(
    @Body destinationDefinitionUpdate: DestinationDefinitionUpdate,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.updateDestinationDefinition(
        destinationDefinitionUpdate,
      )
    }
}
