/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.workers.process.ProcessFactory;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DbtTransformationRunnerTest {

  /**
   * It is simpler to assert the custom transformation prep image is called with the correct
   * arguments. The alternative is to set up an E2E Custom Transformation test, that would, among
   * other things require a separate DBT test repo just for this test.
   */
  @Test
  void configureDbtTest() throws Exception {
    final var processFac = mock(ProcessFactory.class);
    final var process = mock(Process.class);
    when(processFac.create(any(), any(), any(), anyInt(), any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any(),
        any(), any(), any(), any(), any(), any(), any())).thenReturn(process);

    final var inputStream = mock(InputStream.class);
    when(process.getInputStream()).thenReturn(inputStream);
    when(process.getErrorStream()).thenReturn(inputStream);
    when(process.exitValue()).thenReturn(0);

    final var runner = new DbtTransformationRunner(processFac, "airbyte/destination-bigquery:0.1.0");
    final var runnerSpy = spy(runner);

    final var dbtConfig = new OperatorDbt()
        .withGitRepoUrl("test url")
        .withDockerImage("test image");

    final var connId = UUID.randomUUID();
    final var workspaceId = UUID.randomUUID();
    final var path = Path.of("/");
    final var config = mock(JsonNode.class);
    final var resourceReq = mock(ResourceRequirements.class);
    runnerSpy.configureDbt("1", 0, connId, workspaceId, path, config, resourceReq, dbtConfig);

    // The key pieces to verify: 1) the correct integration type is called 2) the correct repo is passed
    // in.
    verify(runnerSpy).runConfigureProcess("1", 0, connId, workspaceId, path, Map.of("destination_config.json", ""),
        resourceReq, "configure-dbt", "--integration-type", "bigquery", "--config", "destination_config.json", "--git-repo", "test url");

  }

  @ParameterizedTest
  @CsvSource({
    "airbyte/destination-bigquery:0.1.0, bigquery",
    "airbyte/destination-snowflake:0.1.0, snowflake",
  })
  void getAirbyteDestinationNameTest(String image, String expected) {
    String name = DbtTransformationRunner.getAirbyteDestinationName(image);
    assertTrue(name.equals(expected));
  }

}
