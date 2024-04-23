/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class SourceDefinitionSpecificationApiTest extends BaseControllerTest {

  @Test
  void testCreateCustomSourceDefinition() throws IOException, JsonValidationException, ConfigNotFoundException {
    Mockito.when(connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(Mockito.any()))
        .thenReturn(new SourceDefinitionSpecificationRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/source_definition_specifications/get";
    testEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdWithWorkspaceId()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new SourceDefinitionIdWithWorkspaceId()),
        HttpStatus.NOT_FOUND);
  }

}
