/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DestinationApi
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationDiscoverRead
import io.airbyte.api.model.generated.DestinationDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.DestinationDiscoverSchemaWriteRequestBody
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.DiscoverCatalogResult
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.PartialDestinationUpdate
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.converters.toApi
import io.airbyte.commons.server.converters.toModel
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.ConnectionId
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
  private val destinationDiscoverService: DestinationDiscoverService,
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
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun createDestination(
    @Body destinationCreate: DestinationCreate,
  ): DestinationRead? = execute { destinationHandler.createDestination(destinationCreate) }

  @Post(uri = "/delete")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun deleteDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.deleteDestination(destinationIdRequestBody)
      null
    }
  }

  @Post(uri = "/discover_schema")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun discoverCatalogForDestination(
    @Body destinationDiscoverReqBody: DestinationDiscoverSchemaRequestBody,
  ): DestinationDiscoverRead? =
    execute {
      destinationDiscoverService
        .getDestinationCatalog(
          ActorId(destinationDiscoverReqBody.destinationId),
          destinationDiscoverReqBody.disableCache,
        ).toApi()
    }

  @Post(uri = "/write_discover_catalog_result")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun writeDestinationDiscoverCatalogResult(
    @Body discoverWriteRequestBody: DestinationDiscoverSchemaWriteRequestBody,
  ): DiscoverCatalogResult? =
    execute {
      DiscoverCatalogResult().catalogId(
        destinationDiscoverService
          .writeDiscoverCatalogResult(
            destinationId = ActorId(discoverWriteRequestBody.destinationId),
            catalog = discoverWriteRequestBody.catalog.toModel(),
            configHash = discoverWriteRequestBody.configurationHash,
            destinationVersion = discoverWriteRequestBody.connectorVersion,
          ).value,
      )
    }

  @Post(uri = "/get_catalog_for_connection")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun getCatalogForConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): DestinationDiscoverRead? =
    execute {
      destinationDiscoverService.getDestinationCatalog(ConnectionId(connectionIdRequestBody.connectionId)).toApi()
    }

  @Post(uri = "/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): DestinationRead? = execute { destinationHandler.getDestination(destinationIdRequestBody) }

  @Post(uri = "/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDestinationsForWorkspace(
    @Body actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody,
  ): DestinationReadList? = execute { destinationHandler.listDestinationsForWorkspace(actorListCursorPaginatedRequestBody) }

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
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun updateDestination(
    @Body destinationUpdate: DestinationUpdate,
  ): DestinationRead? = execute { destinationHandler.updateDestination(destinationUpdate) }

  @Post("/upgrade_version")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
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
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun partialUpdateDestination(
    @Body partialDestinationUpdate: PartialDestinationUpdate,
  ): DestinationRead? = execute { destinationHandler.partialDestinationUpdate(partialDestinationUpdate) }
}
