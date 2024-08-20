/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.EnvConfigs;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.exception.WorkerException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

// todo (cgardens) - these are not truly "unit" tests as they are check resources on the internet.
// we should move them to "integration" tests, when we have facility to do so.
@Slf4j
class DockerProcessFactoryTest {

  private static final Path TEST_ROOT = Path.of("/tmp/airbyte_tests");
  private static final String PROCESS_FACTORY = "process_factory";
  private static final String BUSYBOX = "busybox";
  private static final UUID CONNECTION_ID = null;
  private static final UUID WORKSPACE_ID = null;

  private static final ConnectorResourceRequirements expectedResourceRequirements =
      AirbyteIntegrationLauncher.buildGenericConnectorResourceRequirements(new WorkerConfigs(new EnvConfigs()).getResourceRequirements());

  /**
   * {@link DockerProcessFactoryTest#testImageExists()} will fail if jq is not installed. The logs get
   * swallowed when run from gradle. This test exists to explicitly fail with a clear error message
   * when jq is not installed.
   */
  @Test
  void testJqExists() throws IOException {
    final Process process = new ProcessBuilder("jq", "--version").start();
    final StringBuilder out = new StringBuilder();
    final StringBuilder err = new StringBuilder();
    LineGobbler.gobble(process.getInputStream(), out::append);
    LineGobbler.gobble(process.getErrorStream(), err::append);

    WorkerUtils.gentleClose(process, 1, TimeUnit.MINUTES);

    assertEquals(0, process.exitValue(),
        String.format("Error while checking for jq. STDOUT: %s STDERR: %s Please make sure jq is installed (used by testImageExists)", out, err));
  }

  /**
   * This test will fail if jq is not installed. If run from gradle the log line that mentions the jq
   * issue will be swallowed. The exception is visible if run from intellij or with STDERR logging
   * turned on in gradle.
   */
  @Test
  void testImageExists() throws IOException, WorkerException {
    final Path workspaceRoot = Files.createTempDirectory(Files.createDirectories(TEST_ROOT), PROCESS_FACTORY);

    final DockerProcessFactory processFactory = new DockerProcessFactory(createConfigProviderStub(), workspaceRoot, null, null, null);
    assertTrue(processFactory.checkImageExists(BUSYBOX));
  }

  @Test
  void testImageDoesNotExist() throws IOException, WorkerException {
    final Path workspaceRoot = Files.createTempDirectory(Files.createDirectories(TEST_ROOT), PROCESS_FACTORY);

    final DockerProcessFactory processFactory = new DockerProcessFactory(createConfigProviderStub(), workspaceRoot, null, null, null);
    assertFalse(processFactory.checkImageExists("airbyte/fake:0.1.2"));
  }

  static class DebuggingOptionsTestArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
      final String options = "OPTIONS";
      final String postgresLatest = "repo/project/destination-postgres:latest";
      final String destinationPostgres5005 = "destination-postgres:5005";
      return Stream.of(
          Arguments.of(postgresLatest, destinationPostgres5005, options,
              List.of("-e", "JAVA_TOOL_OPTIONS=OPTIONS:5005", "-p5005:5005")),
          Arguments.of("repo/project/destination-bigquery:latest", destinationPostgres5005, options, Collections.emptyList()),
          Arguments.of(postgresLatest, "destination-postgres:5005,source-postgres:5010", options,
              List.of("-e", "JAVA_TOOL_OPTIONS=OPTIONS:5005", "-p5005:5005")),
          Arguments.of("repo/project/source-postgres:latest", "destination-postgres:5005,source-postgres:5010", options,
              List.of("-e", "JAVA_TOOL_OPTIONS=OPTIONS:5010", "-p5010:5010")),
          Arguments.of(postgresLatest, null, options, Collections.emptyList()),
          // This is the case we'd expect to see in production, no environment variables present
          Arguments.of(postgresLatest, null, null, Collections.emptyList()),
          Arguments.of(postgresLatest, destinationPostgres5005, null, Collections.emptyList()),
          Arguments.of(postgresLatest, "an-obviously-bad-value", options, Collections.emptyList()),
          Arguments.of(postgresLatest, "destination-postgres:a-bad-middle:5005", options, Collections.emptyList()));
    }

  }

  private WorkerConfigsProvider createConfigProviderStub() {
    return createConfigProviderStub(new WorkerConfigs(new EnvConfigs()));
  }

  private WorkerConfigsProvider createConfigProviderStub(final WorkerConfigs workerConfigs) {
    final WorkerConfigsProvider workerConfigsProvider = mock(WorkerConfigsProvider.class);
    when(workerConfigsProvider.getConfig(any())).thenReturn(workerConfigs);
    return workerConfigsProvider;
  }

  @ParameterizedTest
  @ArgumentsSource(DebuggingOptionsTestArgumentsProvider.class)
  void testLocalDebuggingOptions(final String containerName,
                                 final String debugContainerEnvVar,
                                 final String javaOptions,
                                 final List<String> expected) {
    final List<String> actual = DockerProcessFactory.localDebuggingOptions(containerName, debugContainerEnvVar, javaOptions);
    Assertions.assertEquals(actual, expected);
  }

}
