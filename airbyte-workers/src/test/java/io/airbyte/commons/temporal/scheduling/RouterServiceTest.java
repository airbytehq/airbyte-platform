/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;

import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.config.Geography;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigRepository;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test suite for the {@link RouterService} class.
 */
@ExtendWith(MockitoExtension.class)
class RouterServiceTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();

  private static final String US_TASK_QUEUE = "US_TASK_QUEUE";
  private static final String EU_TASK_QUEUE = "EU_TASK_QUEUE";

  private static final String US_FLAGGED_TASK_QUEUE = "US_FLAGGED_TASK_QUEUE";
  private static final String EU_FLAGGED_TASK_QUEUE = "EU_FLAGGED_TASK_QUEUE";

  @Mock(strictness = LENIENT)
  private ConfigRepository mConfigRepository;

  @Mock(strictness = LENIENT)
  private TaskQueueMapper mTaskQueueMapper;

  @Mock
  private FeatureFlags mockFeatureFlag;

  private RouterService routerService;

  @BeforeEach
  void init() {
    routerService = new RouterService(mConfigRepository, mTaskQueueMapper,
        mockFeatureFlag);

    Mockito.when(mConfigRepository.getStandardWorkspaceFromConnection(CONNECTION_ID, false))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID));

    Mockito.when(mTaskQueueMapper.getTaskQueue(eq(Geography.AUTO), any(TemporalJobType.class))).thenReturn(US_TASK_QUEUE);
    Mockito.when(mTaskQueueMapper.getTaskQueue(eq(Geography.US), any(TemporalJobType.class))).thenReturn(US_TASK_QUEUE);
    Mockito.when(mTaskQueueMapper.getTaskQueue(eq(Geography.EU), any(TemporalJobType.class))).thenReturn(EU_TASK_QUEUE);

    Mockito.when(mTaskQueueMapper.getTaskQueueFlagged(eq(Geography.AUTO), any(TemporalJobType.class))).thenReturn(US_FLAGGED_TASK_QUEUE);
    Mockito.when(mTaskQueueMapper.getTaskQueueFlagged(eq(Geography.US), any(TemporalJobType.class))).thenReturn(US_FLAGGED_TASK_QUEUE);
    Mockito.when(mTaskQueueMapper.getTaskQueueFlagged(eq(Geography.EU), any(TemporalJobType.class))).thenReturn(EU_FLAGGED_TASK_QUEUE);
  }

  @Test
  void testGetTaskQueue() throws IOException {
    Mockito.when(mockFeatureFlag.processInGcpDataPlane(WORKSPACE_ID.toString())).thenReturn(false);
    Mockito.when(mConfigRepository.getGeographyForConnection(CONNECTION_ID)).thenReturn(Geography.AUTO);
    assertEquals(US_TASK_QUEUE, routerService.getTaskQueue(CONNECTION_ID, TemporalJobType.SYNC));

    Mockito.when(mConfigRepository.getGeographyForConnection(CONNECTION_ID)).thenReturn(Geography.US);
    assertEquals(US_TASK_QUEUE, routerService.getTaskQueue(CONNECTION_ID, TemporalJobType.SYNC));

    Mockito.when(mConfigRepository.getGeographyForConnection(CONNECTION_ID)).thenReturn(Geography.EU);
    assertEquals(EU_TASK_QUEUE, routerService.getTaskQueue(CONNECTION_ID, TemporalJobType.SYNC));
  }

  @Test
  void testGetTaskQueueBehindFlag() throws IOException {
    Mockito.when(mockFeatureFlag.processInGcpDataPlane(WORKSPACE_ID.toString())).thenReturn(true);

    Mockito.when(mConfigRepository.getGeographyForConnection(CONNECTION_ID)).thenReturn(Geography.AUTO);
    assertEquals(US_FLAGGED_TASK_QUEUE, routerService.getTaskQueue(CONNECTION_ID, TemporalJobType.SYNC));

    Mockito.when(mConfigRepository.getGeographyForConnection(CONNECTION_ID)).thenReturn(Geography.US);
    assertEquals(US_FLAGGED_TASK_QUEUE, routerService.getTaskQueue(CONNECTION_ID, TemporalJobType.SYNC));

    Mockito.when(mConfigRepository.getGeographyForConnection(CONNECTION_ID)).thenReturn(Geography.EU);
    assertEquals(EU_FLAGGED_TASK_QUEUE, routerService.getTaskQueue(CONNECTION_ID, TemporalJobType.SYNC));
  }

  @Test
  void testGetWorkspaceTaskQueue() throws IOException {
    Mockito.when(mockFeatureFlag.processInGcpDataPlane(WORKSPACE_ID.toString())).thenReturn(false);

    Mockito.when(mConfigRepository.getGeographyForWorkspace(WORKSPACE_ID)).thenReturn(Geography.AUTO);
    assertEquals(US_TASK_QUEUE, routerService.getTaskQueueForWorkspace(WORKSPACE_ID, TemporalJobType.CHECK_CONNECTION));

    Mockito.when(mConfigRepository.getGeographyForWorkspace(WORKSPACE_ID)).thenReturn(Geography.US);
    assertEquals(US_TASK_QUEUE, routerService.getTaskQueueForWorkspace(WORKSPACE_ID, TemporalJobType.CHECK_CONNECTION));

    Mockito.when(mConfigRepository.getGeographyForWorkspace(WORKSPACE_ID)).thenReturn(Geography.EU);
    assertEquals(EU_TASK_QUEUE, routerService.getTaskQueueForWorkspace(WORKSPACE_ID, TemporalJobType.CHECK_CONNECTION));
  }

  @Test
  void testGetWorkspaceTaskQueueBehindFlag() throws IOException {
    Mockito.when(mockFeatureFlag.processInGcpDataPlane(WORKSPACE_ID.toString())).thenReturn(true);

    Mockito.when(mConfigRepository.getGeographyForWorkspace(WORKSPACE_ID)).thenReturn(Geography.AUTO);
    assertEquals(US_FLAGGED_TASK_QUEUE, routerService.getTaskQueueForWorkspace(WORKSPACE_ID, TemporalJobType.CHECK_CONNECTION));

    Mockito.when(mConfigRepository.getGeographyForWorkspace(WORKSPACE_ID)).thenReturn(Geography.US);
    assertEquals(US_FLAGGED_TASK_QUEUE, routerService.getTaskQueueForWorkspace(WORKSPACE_ID, TemporalJobType.CHECK_CONNECTION));

    Mockito.when(mConfigRepository.getGeographyForWorkspace(WORKSPACE_ID)).thenReturn(Geography.EU);
    assertEquals(EU_FLAGGED_TASK_QUEUE, routerService.getTaskQueueForWorkspace(WORKSPACE_ID, TemporalJobType.CHECK_CONNECTION));
  }

}
