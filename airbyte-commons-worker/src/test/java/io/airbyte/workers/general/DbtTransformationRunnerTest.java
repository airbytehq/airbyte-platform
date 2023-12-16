/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.workers.process.Metadata.CUSTOM_STEP;
import static io.airbyte.workers.process.Metadata.JOB_TYPE_KEY;
import static io.airbyte.workers.process.Metadata.SYNC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.ProcessFactory;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("JavadocMethod")
@RunWith(MockitoJUnitRunner.class)
class DbtTransformationRunnerTest {

  /**
   * It is simpler to assert the custom transformation prep image is called with the correct
   * arguments. The alternative is to set up an E2E Custom Transformation test, that would, among
   * other things require a separate DBT test repo just for this test.
   */
  @Test
  void configureDbtTest() throws Exception {
    final var processFac = mock(ProcessFactory.class);
    final var process = mock(Process.class);

    final var connId = UUID.randomUUID();
    final var workspaceId = UUID.randomUUID();
    final var path = Path.of("/");
    final var config = Jsons.emptyObject();
    final var resourceReq = new ResourceRequirements();

    when(processFac.create(
        WorkerConfigsProvider.ResourceType.DEFAULT,
        CUSTOM_STEP, "1", 0, connId, workspaceId, path, "airbyte/custom-transformation-prep:1.0", false, false,
        ImmutableMap.of(WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME, Jsons.serialize(config)), null,
        AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(resourceReq),
        null, Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, CUSTOM_STEP),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(), "configure-dbt", "--integration-type", "bigquery", "--config", "destination_config.json", "--git-repo", "test url"))
            .thenReturn(process);

    final var inputStream = mock(InputStream.class);
    when(process.getInputStream()).thenReturn(inputStream);
    when(process.getErrorStream()).thenReturn(inputStream);
    when(process.exitValue()).thenReturn(0);

    final var runner = new DbtTransformationRunner(processFac, "airbyte/destination-bigquery:0.1.0");
    final var runnerSpy = spy(runner);

    final var dbtConfig = new OperatorDbt()
        .withGitRepoUrl("test url")
        .withDockerImage("test image");

    runnerSpy.configureDbt("1", 0, connId, workspaceId, path, config, resourceReq, dbtConfig);

    // The key pieces to verify: 1) the correct integration type is called 2) the correct repo is passed
    // in.
    verify(runnerSpy).runConfigureProcess("1", 0, connId, workspaceId, path, Map.of(WorkerConstants.DESTINATION_CONFIG_JSON_FILENAME, "{}"),
        resourceReq, "configure-dbt", "--integration-type", "bigquery", "--config", "destination_config.json", "--git-repo", "test url");

  }

  @ParameterizedTest
  @CsvSource({
    "airbyte/destination-bigquery:0.1.0, bigquery",
    "airbyte/destination-snowflake:0.1.0, snowflake",
  })
  void getAirbyteDestinationNameTest(String image, String expected) {
    String name = DbtTransformationRunner.getAirbyteDestinationName(image);
    assertEquals(name, expected);
  }

}
