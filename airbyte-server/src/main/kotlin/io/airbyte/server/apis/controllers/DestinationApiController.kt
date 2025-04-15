/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DestinationApi
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.PartialDestinationUpdate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/destinations")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationApiController(
  private val destinationHandler: DestinationHandler,
  private val schedulerHandler: SchedulerHandler,
) : DestinationApi {
  @Post(uri = "/check_connection")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun checkConnectionToDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): CheckConnectionRead? =
    execute {
      schedulerHandler.checkDestinationConnectionFromDestinationId(
        destinationIdRequestBody,
      )
    }

  @Post(uri = "/check_connection_for_update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkConnectionToDestinationForUpdate(
    @Body destinationUpdate: DestinationUpdate,
  ): CheckConnectionRead? =
    execute {
      schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(
        destinationUpdate,
      )
    }

  @Post(uri = "/create")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createDestination(
    @Body destinationCreate: DestinationCreate,
  ): DestinationRead? = execute { destinationHandler.createDestination(destinationCreate) }

  @Post(uri = "/delete")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.deleteDestination(destinationIdRequestBody)
      null
    }
  }

  @Post(uri = "/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): DestinationRead? = execute { destinationHandler.getDestination(destinationIdRequestBody) }

  @Post(uri = "/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): DestinationReadList? = execute { destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody) }

  @Post(uri = "/list_paginated")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationsForWorkspacesPaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): DestinationReadList? =
    execute {
      destinationHandler.listDestinationsForWorkspaces(
        listResourcesForWorkspacesRequestBody,
      )
    }

  @Post(uri = "/search")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun searchDestinations(
    @Body destinationSearch: DestinationSearch?,
  ): DestinationReadList? = execute { destinationHandler.searchDestinations(destinationSearch) }

  @Post(uri = "/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateDestination(
    @Body destinationUpdate: DestinationUpdate,
  ): DestinationRead? = execute { destinationHandler.updateDestination(destinationUpdate) }

  @Post("/upgrade_version")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun upgradeDestinationVersion(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.upgradeDestinationVersion(destinationIdRequestBody)
      null
    }
  }

  @Post(uri = "/partial_update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun partialUpdateDestination(
    @Body partialDestinationUpdate: PartialDestinationUpdate,
  ): DestinationRead? = execute { destinationHandler.partialDestinationUpdate(partialDestinationUpdate) }
}
