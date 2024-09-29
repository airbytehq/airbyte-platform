/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.container_orchestrator.orchestrator.JobOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationTest {

  private String application;
  private JobOrchestrator<?> jobOrchestrator;

  @BeforeEach
  void setup() {
    jobOrchestrator = mock(JobOrchestrator.class);
  }

  @Test
  void testHappyPath() throws Exception {
    final var app = new Application(application, jobOrchestrator);
    final var code = app.run();

    assertEquals(0, code);
    verify(jobOrchestrator).runJob();
  }

  @Test
  void testJobFailedWritesFailedStatus() throws Exception {
    when(jobOrchestrator.runJob()).thenThrow(new Exception());
    final var app = new Application(application, jobOrchestrator);
    final var code = app.run();

    assertEquals(1, code);
    verify(jobOrchestrator).runJob();
  }

}
