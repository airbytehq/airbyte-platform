/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;

import io.airbyte.api.generated.DeclarativeSourceDefinitionsApi;
import io.airbyte.api.model.generated.DeclarativeManifestsReadList;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody;
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/declarative_source_definitions")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class DeclarativeSourceDefinitionsApiController implements DeclarativeSourceDefinitionsApi {

  private final DeclarativeSourceDefinitionsHandler handler;

  public DeclarativeSourceDefinitionsApiController(final DeclarativeSourceDefinitionsHandler handler) {
    this.handler = handler;
  }

  @Override
  @Post(uri = "/create_manifest")
  @Status(HttpStatus.CREATED)
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void createDeclarativeSourceDefinitionManifest(final DeclarativeSourceDefinitionCreateManifestRequestBody requestBody) {
    ApiHelper.execute(() -> {
      handler.createDeclarativeSourceDefinitionManifest(requestBody);
      return null;
    });
  }

  @Override
  @Post(uri = "/update_active_manifest")
  @Status(HttpStatus.NO_CONTENT)
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void updateDeclarativeManifestVersion(UpdateActiveManifestRequestBody requestBody) {
    ApiHelper.execute(() -> {
      handler.updateDeclarativeManifestVersion(requestBody);
      return null;
    });
  }

  @Override
  @Post(uri = "/list_manifests")
  @Status(HttpStatus.OK)
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public DeclarativeManifestsReadList listDeclarativeManifests(
                                                               final ListDeclarativeManifestsRequestBody requestBody) {
    return ApiHelper
        .execute(() -> handler.listManifestVersions(requestBody));
  }

}
