/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.SourceApi;
import io.airbyte.api.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.model.generated.CheckConnectionRead;
import io.airbyte.api.model.generated.DiscoverCatalogResult;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.PartialSourceUpdate;
import io.airbyte.api.model.generated.SourceAutoPropagateChange;
import io.airbyte.api.model.generated.SourceCloneRequestBody;
import io.airbyte.api.model.generated.SourceCreate;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.handlers.SourceHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/sources")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class SourceApiController implements SourceApi {

  private final SchedulerHandler schedulerHandler;
  private final SourceHandler sourceHandler;

  public SourceApiController(final SchedulerHandler schedulerHandler, final SourceHandler sourceHandler) {
    this.schedulerHandler = schedulerHandler;
    this.sourceHandler = sourceHandler;
  }

  @Post("/apply_schema_changes")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public void applySchemaChangeForSource(@Body final SourceAutoPropagateChange sourceAutoPropagateChange) {
    ApiHelper.execute(() -> {
      schedulerHandler.applySchemaChangeForSource(sourceAutoPropagateChange);
      return null;
    });
  }

  @Post("/check_connection")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public CheckConnectionRead checkConnectionToSource(@Body final SourceIdRequestBody sourceIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.checkSourceConnectionFromSourceId(sourceIdRequestBody));
  }

  @Post("/check_connection_for_update")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public CheckConnectionRead checkConnectionToSourceForUpdate(@Body final SourceUpdate sourceUpdate) {
    return ApiHelper.execute(() -> schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate));
  }

  @Post("/clone")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceRead cloneSource(@Body final SourceCloneRequestBody sourceCloneRequestBody) {
    return ApiHelper.execute(() -> sourceHandler.cloneSource(sourceCloneRequestBody));
  }

  @Post("/create")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceRead createSource(@Body final SourceCreate sourceCreate) {
    return ApiHelper.execute(() -> sourceHandler.createSourceWithOptionalSecret(sourceCreate));
  }

  @Post("/delete")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void deleteSource(@Body final SourceIdRequestBody sourceIdRequestBody) {
    ApiHelper.execute(() -> {
      sourceHandler.deleteSource(sourceIdRequestBody);
      return null;
    });
  }

  @Post("/discover_schema")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public SourceDiscoverSchemaRead discoverSchemaForSource(@Body final SourceDiscoverSchemaRequestBody sourceDiscoverSchemaRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.discoverSchemaForSourceFromSourceId(sourceDiscoverSchemaRequestBody));
  }

  @Post("/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceRead getSource(@Body final SourceIdRequestBody sourceIdRequestBody) {
    return ApiHelper.execute(() -> sourceHandler.getSource(sourceIdRequestBody));
  }

  @Post("/most_recent_source_actor_catalog")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ActorCatalogWithUpdatedAt getMostRecentSourceActorCatalog(@Body final SourceIdRequestBody sourceIdRequestBody) {
    return ApiHelper.execute(() -> sourceHandler.getMostRecentSourceActorCatalogWithUpdatedAt(sourceIdRequestBody));
  }

  @Post("/list")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceReadList listSourcesForWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody));
  }

  @Post(uri = "/list_paginated")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceReadList listSourcesForWorkspacePaginated(@Body final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> sourceHandler.listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody));
  }

  @Post("/search")
  @Override
  public SourceReadList searchSources(@Body final SourceSearch sourceSearch) {
    return ApiHelper.execute(() -> sourceHandler.searchSources(sourceSearch));
  }

  @Post("/update")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceRead updateSource(@Body final SourceUpdate sourceUpdate) {
    return ApiHelper.execute(() -> sourceHandler.updateSource(sourceUpdate));
  }

  @Post("/upgrade_version")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @Override
  public void upgradeSourceVersion(@Body final SourceIdRequestBody sourceIdRequestBody) {
    ApiHelper.execute(() -> {
      sourceHandler.upgradeSourceVersion(sourceIdRequestBody);
      return null;
    });
  }

  @Post("/partial_update")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceRead partialUpdateSource(@Body final PartialSourceUpdate partialSourceUpdate) {
    return ApiHelper.execute(() -> sourceHandler.updateSourceWithOptionalSecret(partialSourceUpdate));
  }

  @Post("/write_discover_catalog_result")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DiscoverCatalogResult writeDiscoverCatalogResult(@Body final SourceDiscoverSchemaWriteRequestBody request) {
    return ApiHelper.execute(() -> sourceHandler.writeDiscoverCatalogResult(request));
  }

}
