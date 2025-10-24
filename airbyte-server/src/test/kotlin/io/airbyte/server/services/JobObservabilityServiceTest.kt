/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.config.JobConfig
import io.airbyte.config.JobResetConnectionConfig
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.RefreshConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.UUID

class JobObservabilityServiceTest {
  companion object {
    private val SYNC_DESTINATION_VERSION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111")
    private val SYNC_SOURCE_VERSION_ID = UUID.fromString("22222222-2222-2222-2222-222222222222")
    private val SYNC_WORKSPACE_ID = UUID.fromString("33333333-3333-3333-3333-333333333333")

    private val RESET_DESTINATION_VERSION_ID = UUID.fromString("44444444-4444-4444-4444-444444444444")
    private val RESET_WORKSPACE_ID = UUID.fromString("55555555-5555-5555-5555-555555555555")

    private val REFRESH_DESTINATION_VERSION_ID = UUID.fromString("66666666-6666-6666-6666-666666666666")
    private val REFRESH_SOURCE_VERSION_ID = UUID.fromString("77777777-7777-7777-7777-777777777777")
    private val REFRESH_WORKSPACE_ID = UUID.fromString("88888888-8888-8888-8888-888888888888")

    private val UUID_ZERO = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }

  @Test
  fun `test getDestinationDefinitionVersionId with sync config`() {
    val syncConfig = JobSyncConfig()
    syncConfig.destinationDefinitionVersionId = SYNC_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(SYNC_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId with resetConnection config`() {
    val resetConfig = JobResetConnectionConfig()
    resetConfig.destinationDefinitionVersionId = RESET_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(RESET_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId with refresh config`() {
    val refreshConfig = RefreshConfig()
    refreshConfig.destinationDefinitionVersionId = REFRESH_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(REFRESH_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId with no config returns UUID_ZERO`() {
    val jobConfig = JobConfig()

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(UUID_ZERO, result)
  }

  @Test
  fun `test getDestinationDefinitionVersionId prioritizes sync over resetConnection`() {
    val syncConfig = JobSyncConfig()
    syncConfig.destinationDefinitionVersionId = SYNC_DESTINATION_VERSION_ID

    val resetConfig = JobResetConnectionConfig()
    resetConfig.destinationDefinitionVersionId = RESET_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getDestinationDefinitionVersionId(jobConfig)

    assertEquals(SYNC_DESTINATION_VERSION_ID, result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with sync config`() {
    val syncConfig = JobSyncConfig()
    syncConfig.sourceDefinitionVersionId = SYNC_SOURCE_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertEquals(SYNC_SOURCE_VERSION_ID, result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with refresh config`() {
    val refreshConfig = RefreshConfig()
    refreshConfig.sourceDefinitionVersionId = REFRESH_SOURCE_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertEquals(REFRESH_SOURCE_VERSION_ID, result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with resetConnection config returns null`() {
    val resetConfig = JobResetConnectionConfig()
    resetConfig.destinationDefinitionVersionId = RESET_DESTINATION_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertNull(result)
  }

  @Test
  fun `test getSourceDefinitionVersionId with no config returns null`() {
    val jobConfig = JobConfig()

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertNull(result)
  }

  @Test
  fun `test getSourceDefinitionVersionId prioritizes sync over refresh`() {
    val syncConfig = JobSyncConfig()
    syncConfig.sourceDefinitionVersionId = SYNC_SOURCE_VERSION_ID

    val refreshConfig = RefreshConfig()
    refreshConfig.sourceDefinitionVersionId = REFRESH_SOURCE_VERSION_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getSourceDefinitionVersionId(jobConfig)

    assertEquals(SYNC_SOURCE_VERSION_ID, result)
  }

  @Test
  fun `test getWorkspaceId with sync config`() {
    val syncConfig = JobSyncConfig()
    syncConfig.workspaceId = SYNC_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(SYNC_WORKSPACE_ID, result)
  }

  @Test
  fun `test getWorkspaceId with resetConnection config`() {
    val resetConfig = JobResetConnectionConfig()
    resetConfig.workspaceId = RESET_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.resetConnection = resetConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(RESET_WORKSPACE_ID, result)
  }

  @Test
  fun `test getWorkspaceId with refresh config`() {
    val refreshConfig = RefreshConfig()
    refreshConfig.workspaceId = REFRESH_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(REFRESH_WORKSPACE_ID, result)
  }

  @Test
  fun `test getWorkspaceId with no config returns UUID_ZERO`() {
    val jobConfig = JobConfig()

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(UUID_ZERO, result)
  }

  @Test
  fun `test getWorkspaceId prioritizes sync over resetConnection and refresh`() {
    val syncConfig = JobSyncConfig()
    syncConfig.workspaceId = SYNC_WORKSPACE_ID

    val resetConfig = JobResetConnectionConfig()
    resetConfig.workspaceId = RESET_WORKSPACE_ID

    val refreshConfig = RefreshConfig()
    refreshConfig.workspaceId = REFRESH_WORKSPACE_ID

    val jobConfig = JobConfig()
    jobConfig.sync = syncConfig
    jobConfig.resetConnection = resetConfig
    jobConfig.refresh = refreshConfig

    val result = JobObservabilityService.getWorkspaceId(jobConfig)

    assertEquals(SYNC_WORKSPACE_ID, result)
  }
}
