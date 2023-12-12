/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link DefaultPostLoadExecutor} class.
 */
class DefaultPostLoadExecutorTest {

  @Test
  void testPostLoadExecution()
      throws Exception {
    final ApplyDefinitionsHelper applyDefinitionsHelper = mock(ApplyDefinitionsHelper.class);

    final DeclarativeSourceUpdater declarativeSourceUpdater = mock(DeclarativeSourceUpdater.class);

    final DefaultPostLoadExecutor postLoadExecution =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater);

    assertDoesNotThrow(() -> postLoadExecution.execute());
    verify(applyDefinitionsHelper, times(1)).apply();
  }

  @Test
  void testPostLoadExecutionWithException() throws JsonValidationException, IOException, ConfigNotFoundException {
    final ApplyDefinitionsHelper applyDefinitionsHelper = mock(ApplyDefinitionsHelper.class);
    final DeclarativeSourceUpdater declarativeSourceUpdater = mock(DeclarativeSourceUpdater.class);

    doThrow(new IOException("test")).when(applyDefinitionsHelper).apply();

    final DefaultPostLoadExecutor postLoadExecution =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater);

    assertThrows(IOException.class, () -> postLoadExecution.execute());
  }

}
