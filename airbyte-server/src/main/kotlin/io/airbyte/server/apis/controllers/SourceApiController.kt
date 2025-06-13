/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SourceApi
import io.airbyte.api.model.generated.ActorCatalogWithUpdatedAt
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.DiscoverCatalogResult
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceAutoPropagateChange
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceReadList
import io.airbyte.api.model.generated.SourceSearch
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
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

@Controller("/api/v1/sources")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SourceApiController(
  private val schedulerHandler: SchedulerHandler,
  private val sourceHandler: SourceHandler,
) : SourceApi {
  @Post("/apply_schema_changes")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun applySchemaChangeForSource(
    @Body sourceAutoPropagateChange: SourceAutoPropagateChange,
  ) {
    execute<Any?> {
      schedulerHandler.applySchemaChangeForSource(sourceAutoPropagateChange)
      null
    }
  }

  @Post("/check_connection")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun checkConnectionToSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): CheckConnectionRead? = execute { schedulerHandler.checkSourceConnectionFromSourceId(sourceIdRequestBody) }

  @Post("/check_connection_for_update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkConnectionToSourceForUpdate(
    @Body sourceUpdate: SourceUpdate,
  ): CheckConnectionRead? = execute { schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate) }

  @Post("/create")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun createSource(
    @Body sourceCreate: SourceCreate,
  ): SourceRead? = execute { sourceHandler.createSourceWithOptionalSecret(sourceCreate) }

  @Post("/delete")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun deleteSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ) {
    execute<Any?> {
      sourceHandler.deleteSource(sourceIdRequestBody)
      null
    }
  }

  @Post("/discover_schema")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun discoverSchemaForSource(
    @Body sourceDiscoverSchemaRequestBody: SourceDiscoverSchemaRequestBody,
  ): SourceDiscoverSchemaRead? =
    execute {
      schedulerHandler.discoverSchemaForSourceFromSourceId(
        sourceDiscoverSchemaRequestBody,
      )
    }

  @Post("/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSource(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): SourceRead? = execute { sourceHandler.getSource(sourceIdRequestBody) }

  @Post("/most_recent_source_actor_catalog")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getMostRecentSourceActorCatalog(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): ActorCatalogWithUpdatedAt? =
    execute {
      sourceHandler.getMostRecentSourceActorCatalogWithUpdatedAt(
        sourceIdRequestBody,
      )
    }

  @Post("/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listSourcesForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): SourceReadList? = execute { sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody) }

  @Post(uri = "/list_paginated")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listSourcesForWorkspacePaginated(
    @Body listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody,
  ): SourceReadList? = execute { sourceHandler.listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody) }

  @Post("/search")
  override fun searchSources(
    @Body sourceSearch: SourceSearch?,
  ): SourceReadList? = execute { sourceHandler.searchSources(sourceSearch) }

  @Post("/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun updateSource(
    @Body sourceUpdate: SourceUpdate,
  ): SourceRead? = execute { sourceHandler.updateSource(sourceUpdate) }

  @Post("/upgrade_version")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun upgradeSourceVersion(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ) {
    execute<Any?> {
      sourceHandler.upgradeSourceVersion(sourceIdRequestBody)
      null
    }
  }

  @Post("/partial_update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun partialUpdateSource(
    @Body partialSourceUpdate: PartialSourceUpdate,
  ): SourceRead? = execute { sourceHandler.updateSourceWithOptionalSecret(partialSourceUpdate) }

  @Post("/write_discover_catalog_result")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun writeDiscoverCatalogResult(
    @Body request: SourceDiscoverSchemaWriteRequestBody,
  ): DiscoverCatalogResult? = execute { sourceHandler.writeDiscoverCatalogResult(request) }
}
