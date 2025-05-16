/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DestinationDefinitionApi
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.api.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList
import io.airbyte.api.model.generated.ScopeType
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

@Controller("/api/v1/destination_definitions")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationDefinitionApiController(
  private val destinationDefinitionsHandler: DestinationDefinitionsHandler,
  private val accessValidator: ActorDefinitionAccessValidator,
) : DestinationDefinitionApi {
  @Post(uri = "/create_custom")
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createCustomDestinationDefinition(
    @Body customDestinationDefinitionCreate: CustomDestinationDefinitionCreate,
  ): DestinationDefinitionRead? {
    // legacy calls contain workspace id instead of scope id and scope type
    if (customDestinationDefinitionCreate.workspaceId != null) {
      customDestinationDefinitionCreate.setScopeType(ScopeType.WORKSPACE)
      customDestinationDefinitionCreate.scopeId = customDestinationDefinitionCreate.workspaceId
    }
    return execute {
      destinationDefinitionsHandler.createCustomDestinationDefinition(
        customDestinationDefinitionCreate,
      )
    }
  }

  @Post(uri = "/delete") // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  override fun deleteDestinationDefinition(
    @Body destinationDefinitionIdRequestBody: DestinationDefinitionIdRequestBody,
  ) {
    accessValidator.validateWriteAccess(destinationDefinitionIdRequestBody.destinationDefinitionId)
    execute<Any?> {
      destinationDefinitionsHandler.deleteDestinationDefinition(destinationDefinitionIdRequestBody.destinationDefinitionId)
      null
    }
  }

  @Post(uri = "/get")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinition(
    @Body destinationDefinitionIdRequestBody: DestinationDefinitionIdRequestBody,
  ): DestinationDefinitionRead? =
    execute {
      destinationDefinitionsHandler.getDestinationDefinition(
        destinationDefinitionIdRequestBody.destinationDefinitionId,
        true,
      )
    }

  @Post("/get_for_scope")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinitionForScope(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): DestinationDefinitionRead? =
    execute {
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScope,
      )
    }

  @Post(uri = "/get_for_workspace")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinitionForWorkspace(
    @Body destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
  ): DestinationDefinitionRead? =
    execute {
      destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(
        destinationDefinitionIdWithWorkspaceId,
      )
    }

  @Post(uri = "/grant_definition")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun grantDestinationDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): PrivateDestinationDefinitionRead? =
    execute {
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
        actorDefinitionIdWithScope,
      )
    }

  @Post(uri = "/list")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationDefinitions(): DestinationDefinitionReadList? =
    execute(
      Callable {
        destinationDefinitionsHandler.listDestinationDefinitions()
      },
    )

  @Post(uri = "/list_for_workspace")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationDefinitionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): io.airbyte.api.model.generated.DestinationDefinitionReadList? =
    execute {
      destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(
        workspaceIdRequestBody,
      )
    }

  @Post(uri = "/list_latest")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listLatestDestinationDefinitions(): DestinationDefinitionReadList? =
    execute(
      Callable {
        destinationDefinitionsHandler.listLatestDestinationDefinitions()
      },
    )

  @Post(uri = "/list_private")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPrivateDestinationDefinitions(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): PrivateDestinationDefinitionReadList? =
    execute {
      destinationDefinitionsHandler.listPrivateDestinationDefinitions(
        workspaceIdRequestBody,
      )
    }

  @Post(uri = "/revoke_definition")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun revokeDestinationDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ) {
    execute<Any?> {
      destinationDefinitionsHandler.revokeDestinationDefinition(actorDefinitionIdWithScope)
      null
    }
  }

  @Post(uri = "/update") // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDestinationDefinition(
    @Body destinationDefinitionUpdate: DestinationDefinitionUpdate,
  ): DestinationDefinitionRead? {
    accessValidator.validateWriteAccess(destinationDefinitionUpdate.destinationDefinitionId)
    return execute {
      destinationDefinitionsHandler.updateDestinationDefinition(
        destinationDefinitionUpdate,
      )
    }
  }
}
