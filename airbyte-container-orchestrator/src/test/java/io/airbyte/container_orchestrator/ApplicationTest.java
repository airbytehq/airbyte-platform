/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.container_orchestrator.orchestrator.JobOrchestrator;
import io.airbyte.workers.process.AsyncKubePodStatus;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationTest {

  private String application;
  private JobOrchestrator<?> jobOrchestrator;
  private Optional<AsyncStateManager> asyncStateManager;

  @BeforeEach
  void setup() {
    jobOrchestrator = mock(JobOrchestrator.class);
    asyncStateManager = Optional.of(mock(AsyncStateManager.class));
  }

  @Test
  void testHappyPath() throws Exception {
    final var output = "job-output";
    when(jobOrchestrator.runJob()).thenReturn(Optional.of(output));

    final var app = new Application(application, jobOrchestrator, asyncStateManager);
    final var code = app.run();

    assertEquals(0, code);
    verify(jobOrchestrator).runJob();
    verify(asyncStateManager.get()).write(AsyncKubePodStatus.INITIALIZING);
    // NOTE: we don't expect it to write RUNNING, because the job orchestrator is responsible for that.
    verify(asyncStateManager.get()).write(AsyncKubePodStatus.SUCCEEDED, output);
  }

  @Test
  void testJobFailedWritesFailedStatus() throws Exception {
    when(jobOrchestrator.runJob()).thenThrow(new Exception());
    final var app = new Application(application, jobOrchestrator, asyncStateManager);
    final var code = app.run();

    assertEquals(1, code);
    verify(jobOrchestrator).runJob();
    verify(asyncStateManager.get()).write(AsyncKubePodStatus.INITIALIZING);
    // NOTE: we don't expect it to write RUNNING, because the job orchestrator is responsible for that.
    verify(asyncStateManager.get()).write(AsyncKubePodStatus.FAILED);
  }

  @Test
  void testWorkerV2Path() throws Exception {
    asyncStateManager = Optional.empty();

    final var app = new Application(application, jobOrchestrator, asyncStateManager);
    final var code = app.run();

    assertEquals(0, code);
    verify(jobOrchestrator).runJob();
  }

}
