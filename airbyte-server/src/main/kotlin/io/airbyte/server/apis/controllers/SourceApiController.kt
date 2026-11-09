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
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.ConfigSearchRule
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
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
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

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceWriteRule::class,
  )
  override fun checkConnectionToSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): CheckConnectionRead = execute { schedulerHandler.checkSourceConnectionFromSourceId(sourceIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceWriteRule::class,
  )
  override fun checkConnectionToSourceForUpdate(
    @Body sourceUpdate: SourceUpdate,
  ): CheckConnectionRead = execute { schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceWriteRule::class,
  )
  override fun createSource(
    @Body sourceCreate: SourceCreate,
  ): SourceRead = execute { sourceHandler.createSourceWithOptionalSecret(sourceCreate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
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

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceWriteRule::class,
  )
  override fun discoverSchemaForSource(
    @Body sourceDiscoverSchemaRequestBody: SourceDiscoverSchemaRequestBody,
  ): SourceDiscoverSchemaRead = execute { schedulerHandler.discoverSchemaForSourceFromSourceId(sourceDiscoverSchemaRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceReadOrWorkerRule::class,
  )
  override fun getSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): SourceRead = execute { sourceHandler.getSource(sourceIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceReadRule::class,
  )
  override fun listSourcesForWorkspace(
    @Body actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody,
  ): SourceReadList = execute { sourceHandler.listSourcesForWorkspace(actorListCursorPaginatedRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceReadRule::class,
  )
  override fun listSourcesForWorkspacePaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): SourceReadList = execute { sourceHandler.listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody) }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = ConfigSearchRule::class,
  )
  override fun searchSources(
    @Body sourceSearch: SourceSearch,
  ): SourceReadList = execute { sourceHandler.searchSources(sourceSearch) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceWriteOrWorkerRule::class,
  )
  override fun updateSource(
    @Body sourceUpdate: SourceUpdate,
  ): SourceRead = execute { sourceHandler.updateSource(sourceUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.CONNECTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = SourceWriteRule::class,
  )
  override fun partialUpdateSource(
    @Body partialSourceUpdate: PartialSourceUpdate,
  ): SourceRead = execute { sourceHandler.updateSourceWithOptionalSecret(partialSourceUpdate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = WriteDiscoverCatalogResultRule::class,
  )
  override fun writeDiscoverCatalogResult(
    @Body sourceDiscoverSchemaWriteRequestBody: SourceDiscoverSchemaWriteRequestBody,
  ): DiscoverCatalogResult =
    execute {
      sourceHandler.writeDiscoverCatalogResult(sourceDiscoverSchemaWriteRequestBody)
    }
}
