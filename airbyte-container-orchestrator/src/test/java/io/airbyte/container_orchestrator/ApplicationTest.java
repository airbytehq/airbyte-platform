/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.container_orchestrator.orchestrator.ReplicationJobOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationTest {

  private ReplicationJobOrchestrator jobOrchestrator;

  @BeforeEach
  void setup() {
    jobOrchestrator = mock(ReplicationJobOrchestrator.class);
  }

  @Test
  void testHappyPath() throws Exception {
    final var app = new Application(jobOrchestrator);
    final var code = app.run();

    assertEquals(0, code);
    verify(jobOrchestrator).runJob();
  }

  @Test
  void testJobFailedWritesFailedStatus() throws Exception {
    when(jobOrchestrator.runJob()).thenThrow(new Exception());
    final var app = new Application(jobOrchestrator);
    final var code = app.run();

    assertEquals(1, code);
    verify(jobOrchestrator).runJob();
  }

}
