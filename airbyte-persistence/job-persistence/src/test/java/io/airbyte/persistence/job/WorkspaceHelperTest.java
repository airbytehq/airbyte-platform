/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WorkspaceHelperTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DEST_DEFINITION_ID = UUID.randomUUID();
  private static final UUID DEST_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID OPERATION_ID = UUID.randomUUID();
  private static final SourceConnection SOURCE = new SourceConnection()
      .withSourceId(SOURCE_ID)
      .withSourceDefinitionId(SOURCE_DEFINITION_ID)
      .withWorkspaceId(WORKSPACE_ID)
      .withConfiguration(Jsons.deserialize("{}"))
      .withName("source")
      .withTombstone(false);
  private static final DestinationConnection DEST = new DestinationConnection()
      .withDestinationId(DEST_ID)
      .withDestinationDefinitionId(DEST_DEFINITION_ID)
      .withWorkspaceId(WORKSPACE_ID)
      .withConfiguration(Jsons.deserialize("{}"))
      .withName("dest")
      .withTombstone(false);
  private static final StandardSync CONNECTION = new StandardSync()
      .withName("a name")
      .withConnectionId(CONNECTION_ID)
      .withSourceId(SOURCE_ID)
      .withDestinationId(DEST_ID).withCatalog(new ConfiguredAirbyteCatalog().withStreams(new ArrayList<>()))
      .withManual(true);
  private static final StandardSyncOperation OPERATION = new StandardSyncOperation()
      .withOperationId(OPERATION_ID)
      .withWorkspaceId(WORKSPACE_ID)
      .withName("the new normal")
      .withTombstone(false);

  JobPersistence jobPersistence;
  WorkspaceHelper workspaceHelper;
  private SourceService sourceService;
  private DestinationService destinationService;
  private ConnectionService connectionService;
  private OperationService operationService;
  private WorkspaceService workspaceService;

  @BeforeEach
  void setup() throws IOException, JsonValidationException, ConfigNotFoundException {
    jobPersistence = mock(JobPersistence.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);
    connectionService = mock(ConnectionService.class);
    workspaceService = mock(WorkspaceService.class);
    operationService = mock(OperationService.class);

    when(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(SOURCE);
    when(sourceService.getSourceConnection(not(eq(SOURCE_ID)))).thenThrow(ConfigNotFoundException.class);
    when(destinationService.getDestinationConnection(DEST_ID)).thenReturn(DEST);
    when(destinationService.getDestinationConnection(not(eq(DEST_ID)))).thenThrow(ConfigNotFoundException.class);
    when(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(CONNECTION);
    when(connectionService.getStandardSync(not(eq(CONNECTION_ID)))).thenThrow(ConfigNotFoundException.class);
    when(operationService.getStandardSyncOperation(OPERATION_ID)).thenReturn(OPERATION);
    when(operationService.getStandardSyncOperation(not(eq(OPERATION_ID)))).thenThrow(ConfigNotFoundException.class);

    workspaceHelper = new WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService);
  }

  @Test
  void testMissingObjectsRuntimeException() {
    assertThrows(RuntimeException.class, () -> workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(UUID.randomUUID()));
    assertThrows(RuntimeException.class, () -> workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(UUID.randomUUID()));
    assertThrows(RuntimeException.class, () -> workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(UUID.randomUUID()));
    assertThrows(RuntimeException.class, () -> workspaceHelper.getWorkspaceForConnectionIgnoreExceptions(UUID.randomUUID(), UUID.randomUUID()));
    assertThrows(RuntimeException.class, () -> workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(UUID.randomUUID()));
    assertThrows(RuntimeException.class, () -> workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(0L));
  }

  @Test
  void testMissingObjectsProperException() {
    assertThrows(ConfigNotFoundException.class, () -> workspaceHelper.getWorkspaceForSourceId(UUID.randomUUID()));
    assertThrows(ConfigNotFoundException.class, () -> workspaceHelper.getWorkspaceForDestinationId(UUID.randomUUID()));
    assertThrows(ConfigNotFoundException.class, () -> workspaceHelper.getWorkspaceForConnectionId(UUID.randomUUID()));
    assertThrows(ConfigNotFoundException.class, () -> workspaceHelper.getWorkspaceForConnection(UUID.randomUUID(), UUID.randomUUID()));
    assertThrows(ConfigNotFoundException.class, () -> workspaceHelper.getWorkspaceForOperationId(UUID.randomUUID()));
    assertThrows(ConfigNotFoundException.class, () -> workspaceHelper.getWorkspaceForJobId(0L));
  }

  @Test
  @DisplayName("Validate that source caching is working")
  void testSource() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID retrievedWorkspace = workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID);
    assertEquals(WORKSPACE_ID, retrievedWorkspace);
    verify(sourceService, times(1)).getSourceConnection(SOURCE_ID);

    workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID);
    // There should have been no other call to configRepository
    verify(sourceService, times(1)).getSourceConnection(SOURCE_ID);
  }

  @Test
  @DisplayName("Validate that destination caching is working")
  void testDestination() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID retrievedWorkspace = workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID);
    assertEquals(WORKSPACE_ID, retrievedWorkspace);
    verify(destinationService, times(1)).getDestinationConnection(DEST_ID);

    workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID);
    // There should have been no other call to configRepository
    verify(destinationService, times(1)).getDestinationConnection(DEST_ID);
  }

  @Test
  void testConnection() throws IOException, JsonValidationException, ConfigNotFoundException {
    // test retrieving by connection id
    final UUID retrievedWorkspace = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(CONNECTION_ID);
    assertEquals(WORKSPACE_ID, retrievedWorkspace);

    // test retrieving by source and destination ids
    final UUID retrievedWorkspaceBySourceAndDestination = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(CONNECTION_ID);
    assertEquals(WORKSPACE_ID, retrievedWorkspaceBySourceAndDestination);
    verify(connectionService, times(1)).getStandardSync(CONNECTION_ID);

    workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID);
    // There should have been no other call to configRepository
    verify(connectionService, times(1)).getStandardSync(CONNECTION_ID);
  }

  @Test
  void testOperation() throws IOException, JsonValidationException, ConfigNotFoundException {
    // test retrieving by connection id
    final UUID retrievedWorkspace = workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(OPERATION_ID);
    assertEquals(WORKSPACE_ID, retrievedWorkspace);
    verify(operationService, times(1)).getStandardSyncOperation(OPERATION_ID);

    workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(OPERATION_ID);
    verify(operationService, times(1)).getStandardSyncOperation(OPERATION_ID);
  }

  @Test
  void testConnectionAndJobs() throws IOException {
    // test jobs
    final long jobId = 123;
    final Job job = new Job(
        jobId,
        JobConfig.ConfigType.SYNC,
        CONNECTION_ID.toString(),
        new JobConfig().withConfigType(JobConfig.ConfigType.SYNC).withSync(new JobSyncConfig()),
        new ArrayList<>(),
        JobStatus.PENDING,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        true);
    when(jobPersistence.getJob(jobId)).thenReturn(job);

    final UUID jobWorkspace = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId);
    assertEquals(WORKSPACE_ID, jobWorkspace);
  }

}
