/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link ActorDefinitionVersionApiController}.
 */
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class ActorDefinitionVersionApiTest extends BaseControllerTest {

  @Test
  void testGetActorDefinitionForSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(Mockito.any()))
        .thenReturn(new ActorDefinitionVersionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/actor_definition_versions/get_for_source";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new SourceIdRequestBody())),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new SourceIdRequestBody())),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testGetActorDefinitionForDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(Mockito.any()))
        .thenReturn(new ActorDefinitionVersionRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/actor_definition_versions/get_for_destination";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationIdRequestBody())),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new DestinationIdRequestBody())),
        HttpStatus.NOT_FOUND);
  }

}
