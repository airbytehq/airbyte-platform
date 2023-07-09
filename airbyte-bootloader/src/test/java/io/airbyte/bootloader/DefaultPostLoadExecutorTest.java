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

import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.config.init.ApplyDefinitionsHelper;
import io.airbyte.config.init.DeclarativeSourceUpdater;
import io.airbyte.persistence.job.JobPersistence;
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
    final FeatureFlags featureFlags = mock(FeatureFlags.class);
    final JobPersistence jobPersistence = mock(JobPersistence.class);

    final DeclarativeSourceUpdater declarativeSourceUpdater = mock(DeclarativeSourceUpdater.class);

    final DefaultPostLoadExecutor postLoadExecution =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, featureFlags, jobPersistence);

    assertDoesNotThrow(() -> postLoadExecution.execute());
    verify(applyDefinitionsHelper, times(1)).apply();
  }

  @Test
  void testPostLoadExecutionWithException() throws JsonValidationException, IOException {
    final ApplyDefinitionsHelper applyDefinitionsHelper = mock(ApplyDefinitionsHelper.class);
    final DeclarativeSourceUpdater declarativeSourceUpdater = mock(DeclarativeSourceUpdater.class);
    final FeatureFlags featureFlags = mock(FeatureFlags.class);
    final JobPersistence jobPersistence = mock(JobPersistence.class);

    doThrow(new IOException("test")).when(applyDefinitionsHelper).apply();

    final DefaultPostLoadExecutor postLoadExecution =
        new DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, featureFlags, jobPersistence);

    assertThrows(IOException.class, () -> postLoadExecution.execute());
  }

}
