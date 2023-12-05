/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.mockito.Mockito.doThrow;

import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate;
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionRead;
import io.airbyte.api.model.generated.DestinationDefinitionReadList;
import io.airbyte.api.model.generated.DestinationDefinitionUpdate;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead;
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.ApplicationErrorKnownException;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class DestinationDefinitionApiTest extends BaseControllerTest {

  @Test
  void testCheckConnectionToDestination() throws IOException {
    Mockito.when(destinationDefinitionsHandler.createCustomDestinationDefinition(Mockito.any()))
        .thenReturn(new DestinationDefinitionRead());
    final String path = "/api/v1/destination_definitions/create_custom";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new CustomDestinationDefinitionCreate())),
        HttpStatus.OK);
  }

  @Test
  void testDeleteDestinationDefinition() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.doNothing()
        .doThrow(new ConfigNotFoundException("", ""))
        .when(destinationDefinitionsHandler).deleteDestinationDefinition(Mockito.any());
    final String path = "/api/v1/destination_definitions/delete";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody())),
        HttpStatus.NO_CONTENT);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testDeleteDestinationDefinitionNoWriteAccess() {
    final UUID destinationDefinitionId = UUID.randomUUID();
    doThrow(new ApplicationErrorKnownException("invalid")).when(actorDefinitionAccessValidator).validateWriteAccess(destinationDefinitionId);

    final String path = "/api/v1/destination_definitions/delete";
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody().destinationDefinitionId(destinationDefinitionId))),
        HttpStatus.UNPROCESSABLE_ENTITY);
  }

  @Test
  void testGetDestinationDefinition() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(destinationDefinitionsHandler.getDestinationDefinition(Mockito.any()))
        .thenReturn(new DestinationDefinitionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/destination_definitions/get";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody())),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetDestinationDefinitionForWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(Mockito.any()))
        .thenReturn(new DestinationDefinitionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/destination_definitions/get_for_workspace";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody())),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdRequestBody())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testGrantDestinationDefinitionToWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(Mockito.any()))
        .thenReturn(new PrivateDestinationDefinitionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/destination_definitions/grant_definition";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdWithWorkspaceId())),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdWithWorkspaceId())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testListDestinationDefinitions() throws JsonValidationException, IOException {
    Mockito.when(destinationDefinitionsHandler.listDestinationDefinitions())
        .thenReturn(new DestinationDefinitionReadList());
    final String path = "/api/v1/destination_definitions/list";
    testEndpointStatus(
        HttpRequest.POST(path, ""),
        HttpStatus.OK);
  }

  @Test
  void testListDestinationDefinitionsForWorkspace() throws IOException {
    Mockito.when(destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(Mockito.any()))
        .thenReturn(new DestinationDefinitionReadList());
    final String path = "/api/v1/destination_definitions/list_for_workspace";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new WorkspaceIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testListLatestDestinationDefinitions() {
    Mockito.when(destinationDefinitionsHandler.listLatestDestinationDefinitions())
        .thenReturn(new DestinationDefinitionReadList());
    final String path = "/api/v1/destination_definitions/list_latest";
    testEndpointStatus(
        HttpRequest.POST(path, ""),
        HttpStatus.OK);
  }

  @Test
  void testListPrivateDestinationDefinitions() throws IOException {
    Mockito.when(destinationDefinitionsHandler.listPrivateDestinationDefinitions(Mockito.any()))
        .thenReturn(new PrivateDestinationDefinitionReadList());
    final String path = "/api/v1/destination_definitions/list_private";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new WorkspaceIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testRevokeDestinationDefinitionFromWorkspace() throws IOException {
    Mockito.doNothing()
        .when(destinationDefinitionsHandler).revokeDestinationDefinition(Mockito.any());
    final String path = "/api/v1/destination_definitions/revoke_definition";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionIdWithWorkspaceId())),
        HttpStatus.OK);
  }

  @Test
  void testUpdateDestinationDefinition() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(destinationDefinitionsHandler.updateDestinationDefinition(Mockito.any()))
        .thenReturn(new DestinationDefinitionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/destination_definitions/update";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionUpdate())),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionUpdate())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testUpdateDestinationDefinitionNoWriteAccess() {
    final UUID destinationDefinitionId = UUID.randomUUID();
    doThrow(new ApplicationErrorKnownException("invalid")).when(actorDefinitionAccessValidator).validateWriteAccess(destinationDefinitionId);

    final String path = "/api/v1/destination_definitions/update";
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationDefinitionUpdate().destinationDefinitionId(destinationDefinitionId))),
        HttpStatus.UNPROCESSABLE_ENTITY);
  }

}
