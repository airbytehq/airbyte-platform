/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SourceDefinitionApi
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.model.generated.EnterpriseSourceStubsReadList
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList
import io.airbyte.api.model.generated.ScopeType
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionUpdate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.handlers.EnterpriseSourceStubsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.concurrent.Callable

@Controller("/api/v1/source_definitions")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourceDefinitionApiController(
  private val sourceDefinitionsHandler: SourceDefinitionsHandler,
  private val enterpriseSourceStubsHandler: EnterpriseSourceStubsHandler,
  private val accessValidator: ActorDefinitionAccessValidator,
) : SourceDefinitionApi {
  @Post("/create_custom")
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createCustomSourceDefinition(
    @Body customSourceDefinitionCreate: CustomSourceDefinitionCreate,
  ): SourceDefinitionRead? {
    // legacy calls contain workspace id instead of scope id and scope type
    if (customSourceDefinitionCreate.workspaceId != null) {
      customSourceDefinitionCreate.setScopeType(ScopeType.WORKSPACE)
      customSourceDefinitionCreate.scopeId = customSourceDefinitionCreate.workspaceId
    }
    return execute {
      sourceDefinitionsHandler.createCustomSourceDefinition(
        customSourceDefinitionCreate,
      )
    }
  }

  @Post("/delete") // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  override fun deleteSourceDefinition(
    @Body sourceDefinitionIdRequestBody: SourceDefinitionIdRequestBody,
  ) {
    log.info("about to call access validator")
    accessValidator.validateWriteAccess(sourceDefinitionIdRequestBody.sourceDefinitionId)
    execute<Any?> {
      sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody.sourceDefinitionId)
      null
    }
  }

  @Post("/get")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSourceDefinition(
    @Body sourceDefinitionIdRequestBody: SourceDefinitionIdRequestBody,
  ): SourceDefinitionRead? =
    execute {
      sourceDefinitionsHandler.getSourceDefinition(
        sourceDefinitionIdRequestBody.sourceDefinitionId,
        true,
      )
    }

  @Post("/get_for_scope")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSourceDefinitionForScope(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): SourceDefinitionRead? =
    execute {
      sourceDefinitionsHandler.getSourceDefinitionForScope(
        actorDefinitionIdWithScope,
      )
    }

  @Post("/get_for_workspace")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSourceDefinitionForWorkspace(
    @Body sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId,
  ): SourceDefinitionRead? =
    execute {
      sourceDefinitionsHandler.getSourceDefinitionForWorkspace(
        sourceDefinitionIdWithWorkspaceId,
      )
    }

  @Post("/grant_definition")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun grantSourceDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ): PrivateSourceDefinitionRead? =
    execute {
      sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(
        actorDefinitionIdWithScope,
      )
    }

  @Post("/list_enterprise_source_stubs")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listEnterpriseSourceStubs(): EnterpriseSourceStubsReadList? =
    execute(
      Callable {
        enterpriseSourceStubsHandler.listEnterpriseSourceStubs()
      },
    )

  @Post("/list_enterprise_stubs_for_workspace")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listEnterpriseSourceStubsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): io.airbyte.api.model.generated.EnterpriseSourceStubsReadList? =
    execute {
      enterpriseSourceStubsHandler.listEnterpriseSourceStubsForWorkspace(
        workspaceIdRequestBody.workspaceId,
      )
    }

  @Post("/list_latest")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listLatestSourceDefinitions(): SourceDefinitionReadList? = execute(Callable { sourceDefinitionsHandler.listLatestSourceDefinitions() })

  @Post("/list_private")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listPrivateSourceDefinitions(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): PrivateSourceDefinitionReadList? =
    execute {
      sourceDefinitionsHandler.listPrivateSourceDefinitions(
        workspaceIdRequestBody,
      )
    }

  @Post("/list")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listSourceDefinitions(): SourceDefinitionReadList? = execute(Callable { sourceDefinitionsHandler.listSourceDefinitions() })

  @Post("/list_for_workspace")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listSourceDefinitionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): io.airbyte.api.model.generated.SourceDefinitionReadList? =
    execute {
      sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(
        workspaceIdRequestBody,
      )
    }

  @Post("/revoke_definition")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  override fun revokeSourceDefinition(
    @Body actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
  ) {
    execute<Any?> {
      sourceDefinitionsHandler.revokeSourceDefinition(actorDefinitionIdWithScope)
      null
    }
  }

  @Post("/update") // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateSourceDefinition(
    @Body sourceDefinitionUpdate: SourceDefinitionUpdate,
  ): SourceDefinitionRead? {
    accessValidator.validateWriteAccess(sourceDefinitionUpdate.sourceDefinitionId)
    return execute { sourceDefinitionsHandler.updateSourceDefinition(sourceDefinitionUpdate) }
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
