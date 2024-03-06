/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.generated.ConnectorBuilderProjectApi;
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead;
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody;
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate;
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody;
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionIdBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/connector_builder_projects")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class ConnectorBuilderProjectApiController implements ConnectorBuilderProjectApi {

  private final ConnectorBuilderProjectsHandler connectorBuilderProjectsHandler;

  public ConnectorBuilderProjectApiController(final ConnectorBuilderProjectsHandler connectorBuilderProjectsHandler) {
    this.connectorBuilderProjectsHandler = connectorBuilderProjectsHandler;
  }

  @Override
  @Post(uri = "/create")
  @Status(HttpStatus.CREATED)
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectorBuilderProjectIdWithWorkspaceId createConnectorBuilderProject(final ConnectorBuilderProjectWithWorkspaceId project) {
    return ApiHelper.execute(() -> connectorBuilderProjectsHandler.createConnectorBuilderProject(project));
  }

  @Override
  @Post(uri = "/delete")
  @Status(HttpStatus.NO_CONTENT)
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void deleteConnectorBuilderProject(final ConnectorBuilderProjectIdWithWorkspaceId connectorBuilderProjectIdWithWorkspaceId) {
    ApiHelper.execute(() -> {
      connectorBuilderProjectsHandler.deleteConnectorBuilderProject(connectorBuilderProjectIdWithWorkspaceId);
      return null;
    });
  }

  @Override
  @Post(uri = "/get_with_manifest")
  @Status(HttpStatus.OK)
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectorBuilderProjectRead getConnectorBuilderProject(final ConnectorBuilderProjectIdWithWorkspaceId project) {
    return ApiHelper.execute(() -> connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(project));
  }

  @Override
  @Post(uri = "/list")
  @Status(HttpStatus.OK)
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectorBuilderProjectReadList listConnectorBuilderProjects(final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> connectorBuilderProjectsHandler.listConnectorBuilderProjects(workspaceIdRequestBody));
  }

  @Override
  @Post(uri = "/publish")
  @Status(HttpStatus.OK)
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  public SourceDefinitionIdBody publishConnectorBuilderProject(final ConnectorBuilderPublishRequestBody connectorBuilderPublishRequestBody) {
    return ApiHelper.execute(() -> connectorBuilderProjectsHandler.publishConnectorBuilderProject(connectorBuilderPublishRequestBody));
  }

  @Override
  @Post(uri = "/read_stream")
  @Status(HttpStatus.OK)
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @SuppressWarnings("LineLength")
  public ConnectorBuilderProjectStreamRead readConnectorBuilderProjectStream(final ConnectorBuilderProjectStreamReadRequestBody connectorBuilderProjectStreamReadRequestBody) {
    return ApiHelper.execute(() -> connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(connectorBuilderProjectStreamReadRequestBody));
  }

  @Override
  @Post(uri = "/update")
  @Status(HttpStatus.NO_CONTENT)
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void updateConnectorBuilderProject(final ExistingConnectorBuilderProjectWithWorkspaceId existingConnectorBuilderProjectWithWorkspaceId) {
    ApiHelper.execute(() -> {
      connectorBuilderProjectsHandler.updateConnectorBuilderProject(existingConnectorBuilderProjectWithWorkspaceId);
      return null;
    });
  }

  @Override
  @Post(uri = "/update_testing_values")
  @Status(HttpStatus.OK)
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @SuppressWarnings("LineLength")
  public JsonNode updateConnectorBuilderProjectTestingValues(final ConnectorBuilderProjectTestingValuesUpdate connectorBuilderProjectTestingValuesUpdate) {
    return ApiHelper
        .execute(() -> connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(connectorBuilderProjectTestingValuesUpdate));
  }

}
