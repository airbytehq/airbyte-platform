/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.config.Configs;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.general.ReplicationWorker;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.PostgreSQLContainer;

@ExtendWith(MockitoExtension.class)
class TemporalAttemptExecutionTest {

  private static final String JOB_ID = "11";
  private static final int ATTEMPT_NUMBER = 1;

  private static final JobRunConfig JOB_RUN_CONFIG = new JobRunConfig().withJobId(JOB_ID).withAttemptId((long) ATTEMPT_NUMBER);
  private static final String SOURCE_USERNAME = "sourceusername";
  private static final String SOURCE_PASSWORD = "hunter2";

  private static PostgreSQLContainer<?> container;
  private static Configs configs;

  private Path jobRoot;

  private ReplicationWorker worker;
  private Consumer<Path> mdcSetter;

  private TemporalAttemptExecution attemptExecution;

  @Mock
  private LogClientManager logClientManager;

  @BeforeAll
  static void setUpAll() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withUsername(SOURCE_USERNAME)
        .withPassword(SOURCE_PASSWORD);
    container.start();
    configs = mock(Configs.class);
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setup() throws IOException {
    final AirbyteApiClient airbyteApiClient = mock(AirbyteApiClient.class);
    final Path workspaceRoot = Files.createTempDirectory(Path.of("/tmp"), "temporal_attempt_execution_test");
    jobRoot = workspaceRoot.resolve(JOB_ID).resolve(String.valueOf(ATTEMPT_NUMBER));

    worker = mock(ReplicationWorker.class);
    mdcSetter = mock(Consumer.class);

    attemptExecution = new TemporalAttemptExecution(
        workspaceRoot,
        JOB_RUN_CONFIG,
        worker,
        new ReplicationInput(),
        mdcSetter,
        configs.getAirbyteVersionOrWarning(),
        logClientManager);
  }

  @AfterAll
  static void tearDownAll() {
    container.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testSuccessfulSupplierRun() throws Exception {
    final ReplicationOutput expected = new ReplicationOutput();
    when(worker.run(any(), any())).thenReturn(expected);

    final ReplicationOutput actual = attemptExecution.get();

    assertEquals(expected, actual);

    verify(worker).run(any(), any());
    verify(mdcSetter, atLeast(1)).accept(jobRoot);
  }

  @Test
  void testThrowsUnCheckedException() throws Exception {
    when(worker.run(any(), any())).thenThrow(new IllegalArgumentException());

    assertThrows(IllegalArgumentException.class, attemptExecution::get);

    verify(worker).run(any(), any());
    verify(mdcSetter).accept(jobRoot);
  }

}
