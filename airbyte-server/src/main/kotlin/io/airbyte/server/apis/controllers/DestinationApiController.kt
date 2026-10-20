/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.apis.DestinationApi
import io.airbyte.api.server.generated.models.ActorListCursorPaginatedRequestBody
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.DestinationCreate
import io.airbyte.api.server.generated.models.DestinationDiscoverRead
import io.airbyte.api.server.generated.models.DestinationDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.DestinationDiscoverSchemaWriteRequestBody
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationReadList
import io.airbyte.api.server.generated.models.DestinationSearch
import io.airbyte.api.server.generated.models.DestinationUpdate
import io.airbyte.api.server.generated.models.DiscoverCatalogResult
import io.airbyte.api.server.generated.models.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.PartialDestinationUpdate
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.json.Jsons
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
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.airbyte.api.model.generated.DestinationIdRequestBody as JavaDestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationUpdate as JavaDestinationUpdate

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationApiController(
  private val destinationHandler: DestinationHandler,
  private val schedulerHandler: SchedulerHandler,
  private val destinationDiscoverService: DestinationDiscoverService,
) : DestinationApi {
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun checkConnectionToDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): CheckConnectionRead =
    execute {
      // SchedulerHandler still uses the Java models
      val checkConnectionRead =
        schedulerHandler.checkDestinationConnectionFromDestinationId(
          Jsons.convertValue(destinationIdRequestBody, JavaDestinationIdRequestBody::class.java),
        )
      Jsons.convertValue(checkConnectionRead, CheckConnectionRead::class.java)
    }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun checkConnectionToDestinationForUpdate(
    @Body destinationUpdate: DestinationUpdate,
  ): CheckConnectionRead =
    execute {
      val checkConnectionRead =
        schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(
          Jsons.convertValue(destinationUpdate, JavaDestinationUpdate::class.java),
        )
      Jsons.convertValue(checkConnectionRead, CheckConnectionRead::class.java)
    }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createDestination(
    @Body destinationCreate: DestinationCreate,
  ): DestinationRead = execute { destinationHandler.createDestination(destinationCreate) }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.deleteDestination(destinationIdRequestBody)
      null
    }
  }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun discoverCatalogForDestination(
    @Body destinationDiscoverReqBody: DestinationDiscoverSchemaRequestBody,
  ): DestinationDiscoverRead =
    execute {
      destinationDiscoverService
        .getDestinationCatalog(
          ActorId(destinationDiscoverReqBody.destinationId),
          destinationDiscoverReqBody.disableCache ?: false,
        ).toApi()
    }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun writeDestinationDiscoverCatalogResult(
    @Body discoverWriteRequestBody: DestinationDiscoverSchemaWriteRequestBody,
  ): DiscoverCatalogResult =
    execute {
      DiscoverCatalogResult(
        catalogId =
          destinationDiscoverService
            .writeDiscoverCatalogResult(
              destinationId = ActorId(requireNotNull(discoverWriteRequestBody.destinationId) { "destinationId is required" }),
              catalog = discoverWriteRequestBody.catalog.toModel(),
              configHash = requireNotNull(discoverWriteRequestBody.configurationHash) { "configurationHash is required" },
              destinationVersion = requireNotNull(discoverWriteRequestBody.connectorVersion) { "connectorVersion is required" },
            ).value,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getCatalogForConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): DestinationDiscoverRead =
    execute {
      destinationDiscoverService.getDestinationCatalog(ConnectionId(connectionIdRequestBody.connectionId)).toApi()
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): DestinationRead = execute { destinationHandler.getDestination(destinationIdRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listDestinationsForWorkspace(
    @Body actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody,
  ): DestinationReadList = execute { destinationHandler.listDestinationsForWorkspace(actorListCursorPaginatedRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listDestinationsForWorkspacesPaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): DestinationReadList =
    execute {
      destinationHandler.listDestinationsForWorkspaces(
        listResourcesForWorkspacesRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun searchDestinations(
    @Body destinationSearch: DestinationSearch,
  ): DestinationReadList = execute { destinationHandler.searchDestinations(destinationSearch) }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateDestination(
    @Body destinationUpdate: DestinationUpdate,
  ): DestinationRead = execute { destinationHandler.updateDestination(destinationUpdate) }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun upgradeDestinationVersion(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.upgradeDestinationVersion(destinationIdRequestBody)
      null
    }
  }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun partialUpdateDestination(
    @Body partialDestinationUpdate: PartialDestinationUpdate,
  ): DestinationRead {
    // the spec leaves destinationId optional, so a missing one gets past deserialization
    if (partialDestinationUpdate.destinationId == null) {
      throw BadRequestProblem(ProblemMessageData().message("destinationId is required"))
    }
    return execute { destinationHandler.partialDestinationUpdate(partialDestinationUpdate) }
  }
}
