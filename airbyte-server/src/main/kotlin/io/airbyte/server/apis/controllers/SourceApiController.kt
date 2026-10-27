/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SourceApi
import io.airbyte.api.server.generated.models.ActorCatalogWithUpdatedAt
import io.airbyte.api.server.generated.models.ActorListCursorPaginatedRequestBody
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.DiscoverCatalogResult
import io.airbyte.api.server.generated.models.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.PartialSourceUpdate
import io.airbyte.api.server.generated.models.SourceAutoPropagateChange
import io.airbyte.api.server.generated.models.SourceCreate
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceIdWithSecretCoordinatesRequestBody
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceReadList
import io.airbyte.api.server.generated.models.SourceReadWithMetadata
import io.airbyte.api.server.generated.models.SourceSearch
import io.airbyte.api.server.generated.models.SourceUpdate
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.SourceDeleteRule
import io.airbyte.server.auth.SourceReadOrWorkerRule
import io.airbyte.server.auth.SourceReadRule
import io.airbyte.server.auth.SourceSchemaChangeRule
import io.airbyte.server.auth.SourceWriteOrWorkerRule
import io.airbyte.server.auth.SourceWriteRule
import io.airbyte.server.auth.WriteDiscoverCatalogResultRule
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourceApiController(
  private val schedulerHandler: SchedulerHandler,
  private val sourceHandler: SourceHandler,
) : SourceApi {
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceSchemaChangeRule::class,
  )
  override fun applySchemaChangeForSource(
    @Body sourceAutoPropagateChange: SourceAutoPropagateChange,
  ) {
    execute<Any?> {
      schedulerHandler.applySchemaChangeForSource(sourceAutoPropagateChange)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteRule::class,
  )
  override fun checkConnectionToSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): CheckConnectionRead = execute { schedulerHandler.checkSourceConnectionFromSourceId(sourceIdRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteRule::class,
  )
  override fun checkConnectionToSourceForUpdate(
    @Body sourceUpdate: SourceUpdate,
  ): CheckConnectionRead = execute { schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteRule::class,
  )
  override fun createSource(
    @Body sourceCreate: SourceCreate,
  ): SourceRead = execute { sourceHandler.createSourceWithOptionalSecret(sourceCreate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceDeleteRule::class,
  )
  override fun deleteSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ) {
    execute<Any?> {
      sourceHandler.deleteSource(sourceIdRequestBody)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteRule::class,
  )
  override fun discoverSchemaForSource(
    @Body sourceDiscoverSchemaRequestBody: SourceDiscoverSchemaRequestBody,
  ): SourceDiscoverSchemaRead = execute { schedulerHandler.discoverSchemaForSourceFromSourceId(sourceDiscoverSchemaRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceReadOrWorkerRule::class,
  )
  override fun getSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): SourceRead = execute { sourceHandler.getSource(sourceIdRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceReadOrWorkerRule::class,
  )
  override fun getSourceWithMetadata(
    @Body sourceIdWithSecretCoordinatesRequestBody: SourceIdWithSecretCoordinatesRequestBody,
  ): SourceReadWithMetadata =
    execute {
      val sourceIdRequestBody = SourceIdRequestBody(sourceId = sourceIdWithSecretCoordinatesRequestBody.sourceId)
      sourceHandler.getSourceWithMetadata(
        sourceIdRequestBody,
        sourceIdWithSecretCoordinatesRequestBody.includeSecretCoordinates ?: false,
      )
    }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceReadRule::class,
  )
  override fun getMostRecentSourceActorCatalog(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): ActorCatalogWithUpdatedAt =
    execute {
      sourceHandler.getMostRecentSourceActorCatalogWithUpdatedAt(
        sourceIdRequestBody,
      )
    }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceReadRule::class,
  )
  override fun listSourcesForWorkspace(
    @Body actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody,
  ): SourceReadList = execute { sourceHandler.listSourcesForWorkspace(actorListCursorPaginatedRequestBody) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceReadRule::class,
  )
  override fun listSourcesForWorkspacePaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): SourceReadList = execute { sourceHandler.listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody) }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun searchSources(
    @Body sourceSearch: SourceSearch,
  ): SourceReadList = execute { sourceHandler.searchSources(sourceSearch) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteOrWorkerRule::class,
  )
  override fun updateSource(
    @Body sourceUpdate: SourceUpdate,
  ): SourceRead = execute { sourceHandler.updateSource(sourceUpdate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteRule::class,
  )
  override fun upgradeSourceVersion(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ) {
    execute<Any?> {
      sourceHandler.upgradeSourceVersion(sourceIdRequestBody)
      null
    }
  }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SourceWriteRule::class,
  )
  override fun partialUpdateSource(
    @Body partialSourceUpdate: PartialSourceUpdate,
  ): SourceRead = execute { sourceHandler.updateSourceWithOptionalSecret(partialSourceUpdate) }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WriteDiscoverCatalogResultRule::class,
  )
  override fun writeDiscoverCatalogResult(
    @Body sourceDiscoverSchemaWriteRequestBody: SourceDiscoverSchemaWriteRequestBody,
  ): DiscoverCatalogResult =
    execute {
      sourceHandler.writeDiscoverCatalogResult(sourceDiscoverSchemaWriteRequestBody)
    }
}
