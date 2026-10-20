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
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
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
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
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

  // the accessValidator will provide additional authorization checks, depending on the Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteSourceDefinition(
    @Body sourceDefinitionIdRequestBody: SourceDefinitionIdRequestBody,
  ) {
    log.info { "about to call access validator" }
    accessValidator.validateWriteAccess(sourceDefinitionIdRequestBody.sourceDefinitionId)
    execute<Any?> {
      sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody.sourceDefinitionId)
      null
    }
  }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
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

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getSourceDefinitionForScope(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): SourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScope,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getSourceDefinitionForWorkspace(
    @Body sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId,
  ): SourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.getSourceDefinitionForWorkspace(
        sourceDefinitionIdWithWorkspaceId,
      )
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun grantSourceDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): PrivateSourceDefinitionRead =
    execute {
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        actorDefinitionIdWithScope,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listEnterpriseSourceStubs(): EnterpriseConnectorStubsReadList =
    execute {
      enterpriseConnectorStubsHandler.listEnterpriseSourceStubs()
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listEnterpriseSourceStubsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): EnterpriseConnectorStubsReadList =
    execute {
      enterpriseConnectorStubsHandler.listEnterpriseSourceStubsForWorkspace(
        workspaceIdRequestBody!!.workspaceId,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listLatestSourceDefinitions(): SourceDefinitionReadList = execute { sourceDefinitionsHandler.listLatestSourceDefinitions() }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listPrivateSourceDefinitions(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): PrivateSourceDefinitionReadList =
    execute {
      sourceDefinitionsHandler.listPrivateSourceDefinitions(
        workspaceIdRequestBody!!,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listSourceDefinitions(): SourceDefinitionReadList = execute { sourceDefinitionsHandler.listSourceDefinitions() }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listSourceDefinitionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdActorDefinitionRequestBody?,
  ): SourceDefinitionReadList =
    execute {
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(
        workspaceIdRequestBody!!,
      )
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun revokeSourceDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ) {
    execute<Any?> {
      sourceDefinitionsHandler.revokeSourceDefinition(actorDefinitionIdWithScope)
      null
    }
  }

  // the accessValidator will provide additional authorization checks, depending on the Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateSourceDefinition(
    @Body sourceDefinitionUpdate: SourceDefinitionUpdate?,
  ): SourceDefinitionRead {
    accessValidator.validateWriteAccess(sourceDefinitionUpdate!!.sourceDefinitionId)
    return execute { sourceDefinitionsHandler.updateSourceDefinition(sourceDefinitionUpdate) }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
