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
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
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
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
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

  // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
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

  @Secured(AuthRoleConstants.AUTHENTICATED_USER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinition(
    @Body destinationDefinitionIdRequestBody: DestinationDefinitionIdRequestBody,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.getDestinationDefinition(
        destinationDefinitionIdRequestBody.destinationDefinitionId,
        true,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinitionForScope(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.getDestinationDefinitionForScope(
        actorDefinitionIdWithScope,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestinationDefinitionForWorkspace(
    @Body destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
  ): DestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(
        destinationDefinitionIdWithWorkspaceId,
      )
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun grantDestinationDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): PrivateDestinationDefinitionRead =
    execute {
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(
        actorDefinitionIdWithScope,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationDefinitions(): DestinationDefinitionReadList =
    execute(
      Callable {
        destinationDefinitionsHandler.listDestinationDefinitions()
      },
    )

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationDefinitionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdActorDefinitionRequestBody?,
  ): DestinationDefinitionReadList =
    execute {
      destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(
        workspaceIdRequestBody!!,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listEnterpriseDestinationStubsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): EnterpriseConnectorStubsReadList =
    execute {
      enterpriseConnectorStubsHandler.listEnterpriseDestinationStubsForWorkspace(
        workspaceIdRequestBody!!.workspaceId,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listLatestDestinationDefinitions(): DestinationDefinitionReadList =
    execute(
      Callable {
        destinationDefinitionsHandler.listLatestDestinationDefinitions()
      },
    )

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPrivateDestinationDefinitions(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody?,
  ): PrivateDestinationDefinitionReadList =
    execute {
      destinationDefinitionsHandler.listPrivateDestinationDefinitions(
        workspaceIdRequestBody!!,
      )
    }

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

  // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDestinationDefinition(
    @Body destinationDefinitionUpdate: DestinationDefinitionUpdate,
  ): DestinationDefinitionRead {
    accessValidator.validateWriteAccess(destinationDefinitionUpdate.destinationDefinitionId)
    return execute {
      destinationDefinitionsHandler.updateDestinationDefinition(
        destinationDefinitionUpdate,
      )
    }
  }
}
