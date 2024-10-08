/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.CheckOperationRead;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.OperationCreate;
import io.airbyte.api.model.generated.OperationIdRequestBody;
import io.airbyte.api.model.generated.OperationRead;
import io.airbyte.api.model.generated.OperationReadList;
import io.airbyte.api.model.generated.OperationUpdate;
import io.airbyte.api.model.generated.OperatorConfiguration;
import io.airbyte.data.exceptions.ConfigNotFoundException;
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
class OperationApiTest extends BaseControllerTest {

  @Test
  void testCheckOperation() {
    Mockito.when(operationsHandler.checkOperation(Mockito.any()))
        .thenReturn(new CheckOperationRead());
    final String path = "/api/v1/operations/check";
    testEndpointStatus(
        HttpRequest.POST(path, new OperatorConfiguration()),
        HttpStatus.OK);
  }

  @Test
  void testCreateOperation() throws IOException, JsonValidationException, ConfigNotFoundException {
    Mockito.when(operationsHandler.createOperation(Mockito.any()))
        .thenReturn(new OperationRead());
    final String path = "/api/v1/operations/create";
    testEndpointStatus(
        HttpRequest.POST(path, new OperationCreate()),
        HttpStatus.OK);
  }

  @Test
  void testDeleteOperation() throws IOException {
    Mockito.doNothing()
        .when(operationsHandler).deleteOperation(Mockito.any());

    final String path = "/api/v1/operations/delete";
    testEndpointStatus(
        HttpRequest.POST(path, new OperationIdRequestBody()),
        HttpStatus.NO_CONTENT);
  }

  @Test
  void testGetOperation() throws IOException, JsonValidationException, ConfigNotFoundException {
    Mockito.when(operationsHandler.getOperation(Mockito.any()))
        .thenReturn(new OperationRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/operations/get";
    testEndpointStatus(
        HttpRequest.POST(path, new OperationIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new OperationIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testListOperationsForConnection() throws IOException, JsonValidationException, ConfigNotFoundException {
    Mockito.when(operationsHandler.listOperationsForConnection(Mockito.any()))
        .thenReturn(new OperationReadList())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/operations/list";
    testEndpointStatus(
        HttpRequest.POST(path, new ConnectionIdRequestBody()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new ConnectionIdRequestBody()),
        HttpStatus.NOT_FOUND);
  }

  @Test
  void testUpdateOperation() throws IOException, JsonValidationException, ConfigNotFoundException {
    Mockito.when(operationsHandler.updateOperation(Mockito.any()))
        .thenReturn(new OperationRead())
        .thenThrow(new ConfigNotFoundException("", ""));
    final String path = "/api/v1/operations/update";
    testEndpointStatus(
        HttpRequest.POST(path, new OperationUpdate()),
        HttpStatus.OK);
    testErrorEndpointStatus(
        HttpRequest.POST(path, new OperationUpdate()),
        HttpStatus.NOT_FOUND);
  }

}
