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
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.converters.toApi
import io.airbyte.commons.server.converters.toModel
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.ConnectionDestinationCatalogReadRule
import io.airbyte.server.auth.DestinationCatalogWriteRule
import io.airbyte.server.auth.DestinationDeleteRule
import io.airbyte.server.auth.DestinationReadOrWorkerRule
import io.airbyte.server.auth.DestinationReadRule
import io.airbyte.server.auth.DestinationWriteRule
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DestinationApiController(
  private val destinationHandler: DestinationHandler,
  private val schedulerHandler: SchedulerHandler,
  private val destinationDiscoverService: DestinationDiscoverService,
) : DestinationApi {
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationWriteRule::class,
  )
  override fun checkConnectionToDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): CheckConnectionRead = execute { schedulerHandler.checkDestinationConnectionFromDestinationId(destinationIdRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationWriteRule::class,
  )
  override fun checkConnectionToDestinationForUpdate(
    @Body destinationUpdate: DestinationUpdate,
  ): CheckConnectionRead = execute { schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(destinationUpdate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationWriteRule::class,
  )
  override fun createDestination(
    @Body destinationCreate: DestinationCreate,
  ): DestinationRead = execute { destinationHandler.createDestination(destinationCreate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationDeleteRule::class,
  )
  override fun deleteDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.deleteDestination(destinationIdRequestBody)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationReadRule::class,
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

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationCatalogWriteRule::class,
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

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ConnectionDestinationCatalogReadRule::class,
  )
  override fun getCatalogForConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): DestinationDiscoverRead =
    execute {
      destinationDiscoverService.getDestinationCatalog(ConnectionId(connectionIdRequestBody.connectionId)).toApi()
    }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationReadOrWorkerRule::class,
  )
  override fun getDestination(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): DestinationRead = execute { destinationHandler.getDestination(destinationIdRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationReadRule::class,
  )
  override fun listDestinationsForWorkspace(
    @Body actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody,
  ): DestinationReadList = execute { destinationHandler.listDestinationsForWorkspace(actorListCursorPaginatedRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationReadRule::class,
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

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationWriteRule::class,
  )
  override fun updateDestination(
    @Body destinationUpdate: DestinationUpdate,
  ): DestinationRead = execute { destinationHandler.updateDestination(destinationUpdate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationWriteRule::class,
  )
  override fun upgradeDestinationVersion(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ) {
    execute<Any?> {
      destinationHandler.upgradeDestinationVersion(destinationIdRequestBody)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = DestinationWriteRule::class,
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
