/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.DeploymentMetadataRead;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs;
import io.micronaut.context.env.Environment;
import java.util.Set;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DeploymentMetadataHandlerTest {

  @ParameterizedTest
  @ValueSource(strings = {Environment.KUBERNETES, Environment.TEST})
  void testRetrievingDeploymentMetadata(final String activeEnvironment) {
    final UUID deploymentId = UUID.randomUUID();
    final String version = "0.1.2";
    final AirbyteVersion airbyteVersion = new AirbyteVersion(version);
    final Configs.DeploymentMode deploymentMode = Configs.DeploymentMode.OSS;
    final DSLContext dslContext = mock(DSLContext.class);
    final Environment environment = mock(Environment.class);
    final Result<org.jooq.Record> result = mock(Result.class);
    final String expectedEnvironment =
        Environment.KUBERNETES.equals(activeEnvironment) ? Configs.WorkerEnvironment.KUBERNETES.name() : Configs.WorkerEnvironment.DOCKER.name();

    when(result.getValue(anyInt(), anyString())).thenReturn(deploymentId);
    when(dslContext.fetch(anyString())).thenReturn(result);
    when(environment.getActiveNames()).thenReturn(Set.of(activeEnvironment));

    final DeploymentMetadataHandler handler = new DeploymentMetadataHandler(airbyteVersion, deploymentMode, dslContext, environment);

    final DeploymentMetadataRead deploymentMetadataRead = handler.getDeploymentMetadata();

    assertEquals(deploymentId, deploymentMetadataRead.getId());
    assertEquals(expectedEnvironment, deploymentMetadataRead.getEnvironment());
    assertEquals(deploymentMode.name(), deploymentMetadataRead.getMode());
    assertEquals(version, deploymentMetadataRead.getVersion());
  }

}
