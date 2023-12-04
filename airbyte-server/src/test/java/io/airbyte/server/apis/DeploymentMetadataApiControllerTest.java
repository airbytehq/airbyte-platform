/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.DeploymentMetadataRead;
import io.airbyte.config.Configs;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class DeploymentMetadataApiControllerTest extends BaseControllerTest {

  @Test
  void testFetchDeploymentMetadata() {
    final DeploymentMetadataRead deploymentMetadataRead = new DeploymentMetadataRead()
        .id(UUID.randomUUID())
        .environment(Configs.WorkerEnvironment.KUBERNETES.name())
        .mode(Configs.DeploymentMode.OSS.name())
        .version("0.2.3");
    when(deploymentMetadataHandler.getDeploymentMetadata()).thenReturn(deploymentMetadataRead);
    final String path = "/api/v1/deployment/metadata";
    testEndpointStatus(
        HttpRequest.POST(path, null),
        HttpStatus.OK);
  }

}
