/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.commons.json.Jsons;
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
class OrganizationApiTest extends BaseControllerTest {

  @Test
  void testGetOrganization() throws JsonValidationException, ConfigNotFoundException, IOException {
    Mockito.when(organizationsHandler.getOrganization(Mockito.any()))
        .thenReturn(new OrganizationRead());
    final String path = "/api/v1/organizations/get";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new OrganizationIdRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testUpdateOrganization() throws Exception {
    Mockito.when(organizationsHandler.updateOrganization(Mockito.any()))
        .thenReturn(new OrganizationRead());
    final String path = "/api/v1/organizations/update";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new OrganizationUpdateRequestBody())),
        HttpStatus.OK);
  }

  @Test
  void testCreateOrganization() throws Exception {
    Mockito.when(organizationsHandler.createOrganization(Mockito.any()))
        .thenReturn(new OrganizationRead());
    final String path = "/api/v1/organizations/create";
    testEndpointStatus(
        HttpRequest.POST(path, Jsons.serialize(new OrganizationCreateRequestBody())),
        HttpStatus.OK);
  }

}
