/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;

import io.airbyte.api.generated.DeclarativeSourceDefinitionsApi;
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody;
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
@SuppressWarnings("MissingJavadocType")
public class DeclarativeSourceDefinitionsApiController implements DeclarativeSourceDefinitionsApi {

  private final DeclarativeSourceDefinitionsHandler handler;

  public DeclarativeSourceDefinitionsApiController(final DeclarativeSourceDefinitionsHandler handler) {
    this.handler = handler;
  }

  @Override
  @Post(uri = "/create_manifest")
  @Status(HttpStatus.CREATED)
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void createDeclarativeSourceDefinitionManifest(DeclarativeSourceDefinitionCreateManifestRequestBody requestBody) {
    ApiHelper.execute(() -> {
      handler.createDeclarativeSourceDefinitionManifest(requestBody);
      return null;
    });
  }

}
