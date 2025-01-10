/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.SourceDefinitionApi;
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope;
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate;
import io.airbyte.api.model.generated.EnterpriseSourceStubsReadList;
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead;
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList;
import io.airbyte.api.model.generated.ScopeType;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionRead;
import io.airbyte.api.model.generated.SourceDefinitionReadList;
import io.airbyte.api.model.generated.SourceDefinitionUpdate;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.auth.generated.Intent;
import io.airbyte.commons.auth.permissions.RequiresIntent;
import io.airbyte.commons.server.handlers.EnterpriseSourceStubsHandler;
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller("/api/v1/source_definitions")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class SourceDefinitionApiController implements SourceDefinitionApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SourceDefinitionsHandler sourceDefinitionsHandler;
  private final EnterpriseSourceStubsHandler enterpriseSourceStubsHandler;
  private final ActorDefinitionAccessValidator accessValidator;

  public SourceDefinitionApiController(final SourceDefinitionsHandler sourceDefinitionsHandler,
                                       final EnterpriseSourceStubsHandler enterpriseSourceStubsHandler,
                                       final ActorDefinitionAccessValidator accessValidator) {
    this.sourceDefinitionsHandler = sourceDefinitionsHandler;
    this.enterpriseSourceStubsHandler = enterpriseSourceStubsHandler;
    this.accessValidator = accessValidator;
  }

  @Post("/create_custom")
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionRead createCustomSourceDefinition(@Body final CustomSourceDefinitionCreate customSourceDefinitionCreate) {
    // legacy calls contain workspace id instead of scope id and scope type
    if (customSourceDefinitionCreate.getWorkspaceId() != null) {
      customSourceDefinitionCreate.setScopeType(ScopeType.WORKSPACE);
      customSourceDefinitionCreate.setScopeId(customSourceDefinitionCreate.getWorkspaceId());
    }
    return ApiHelper.execute(() -> sourceDefinitionsHandler.createCustomSourceDefinition(customSourceDefinitionCreate));
  }

  @Post("/delete")
  // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void deleteSourceDefinition(@Body final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody) {
    log.info("about to call access validator");
    accessValidator.validateWriteAccess(sourceDefinitionIdRequestBody.getSourceDefinitionId());
    ApiHelper.execute(() -> {
      sourceDefinitionsHandler.deleteSourceDefinition(sourceDefinitionIdRequestBody);
      return null;
    });
  }

  @Post("/get")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionRead getSourceDefinition(@Body final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody) {
    return ApiHelper.execute(() -> sourceDefinitionsHandler.getSourceDefinition(sourceDefinitionIdRequestBody));
  }

  @Post("/get_for_scope")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionRead getSourceDefinitionForScope(@Body final ActorDefinitionIdWithScope actorDefinitionIdWithScope) {
    return ApiHelper.execute(() -> sourceDefinitionsHandler.getSourceDefinitionForScope(actorDefinitionIdWithScope));
  }

  @Post("/get_for_workspace")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionRead getSourceDefinitionForWorkspace(@Body final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId) {
    return ApiHelper.execute(() -> sourceDefinitionsHandler.getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId));
  }

  @Post("/grant_definition")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public PrivateSourceDefinitionRead grantSourceDefinition(@Body final ActorDefinitionIdWithScope actorDefinitionIdWithScope) {
    return ApiHelper.execute(() -> sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(actorDefinitionIdWithScope));
  }

  @Post("/list_enterprise_source_stubs")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public EnterpriseSourceStubsReadList listEnterpriseSourceStubs() {
    return ApiHelper.execute(enterpriseSourceStubsHandler::listEnterpriseSourceStubs);
  }

  @Post("/list_latest")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionReadList listLatestSourceDefinitions() {
    return ApiHelper.execute(sourceDefinitionsHandler::listLatestSourceDefinitions);
  }

  @Post("/list_private")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public PrivateSourceDefinitionReadList listPrivateSourceDefinitions(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> sourceDefinitionsHandler.listPrivateSourceDefinitions(workspaceIdRequestBody));
  }

  @Post("/list")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionReadList listSourceDefinitions() {
    return ApiHelper.execute(sourceDefinitionsHandler::listSourceDefinitions);
  }

  @Post("/list_for_workspace")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionReadList listSourceDefinitionsForWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(workspaceIdRequestBody));
  }

  @Post("/revoke_definition")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void revokeSourceDefinition(@Body final ActorDefinitionIdWithScope actorDefinitionIdWithScope) {
    ApiHelper.execute(() -> {
      sourceDefinitionsHandler.revokeSourceDefinition(actorDefinitionIdWithScope);
      return null;
    });
  }

  @Post("/update")
  // the accessValidator will provide additional authorization checks, depending on Airbyte edition.
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public SourceDefinitionRead updateSourceDefinition(@Body final SourceDefinitionUpdate sourceDefinitionUpdate) {
    accessValidator.validateWriteAccess(sourceDefinitionUpdate.getSourceDefinitionId());
    return ApiHelper.execute(() -> sourceDefinitionsHandler.updateSourceDefinition(sourceDefinitionUpdate));
  }

}
