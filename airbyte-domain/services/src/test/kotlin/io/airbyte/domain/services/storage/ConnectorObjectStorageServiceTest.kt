/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.storage

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.StandardSync
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.domain.models.ConnectionId
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID

class ConnectorObjectStorageServiceTest {
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val connectionService = mockk<ConnectionService>()
  private val destinationService = mockk<DestinationService>()
  private val objectMapper = ObjectMapper()

  private lateinit var connectorObjectStorageService: ConnectorObjectStorageService

  @BeforeEach
  fun setUp() {
    connectorObjectStorageService =
      ConnectorObjectStorageService(
        actorDefinitionService,
        connectionService,
        destinationService,
      )
  }

  @Nested
  inner class GetRejectedRecordsForJob {
    private val connectionId = ConnectionId(UUID.randomUUID())
    private val destinationId = UUID.randomUUID()
    private val destinationVersionId = UUID.randomUUID()
    private val standardSync =
      StandardSync()
        .withConnectionId(connectionId.value)
        .withDestinationId(destinationId)
    private val jobId = 123L
    private val dataActivationVersion = ActorDefinitionVersion().withSupportsDataActivation(true)
    private val configuredDestination =
      DestinationConnection()
        .withConfiguration(
          Jsons.jsonNode(
            mapOf(
              "object_storage_config" to
                mapOf(
                  "storage_type" to "S3",
                  "s3_bucket_name" to "my-bucket",
                  "s3_bucket_region" to "us-east-1",
                  "bucket_path" to "my/path",
                ),
            ),
          ),
        )
    private val job =
      Job(
        jobId,
        JobConfig.ConfigType.SYNC,
        UUID.randomUUID().toString(),
        mockk<JobConfig> {
          every { configType } returns JobConfig.ConfigType.SYNC
          every { sync } returns
            mockk<JobSyncConfig> {
              every { destinationDefinitionVersionId } returns destinationVersionId
            }
        },
        emptyList(),
        JobStatus.PENDING,
        null,
        0L,
        0L,
        true,
      )

    @BeforeEach
    fun setup() {
      every { destinationService.getDestinationConnection(destinationId) } returns configuredDestination
      every { actorDefinitionService.getActorDefinitionVersion(destinationVersionId) } returns dataActivationVersion
      every { connectionService.getStandardSync(connectionId.value) } returns standardSync
    }

    @Test
    fun `returns metadata for valid storage config`() {
      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)

      assertNotNull(result)
      assertEquals("s3://my-bucket/my/path/$jobId/", result!!.storageUri)
      assertEquals(
        "https://us-east-1.console.aws.amazon.com/s3/buckets/my-bucket?prefix=my%2Fpath%2F$jobId%2F",
        result.cloudConsoleUrl,
      )
    }

    @Test
    fun `returns null when rejected record count is zero`() {
      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 0L)
      assertNull(result)
    }

    @Test
    fun `returns null when job is not a sync job`() {
      val job =
        job.copy(
          config =
            mockk<JobConfig> {
              every { configType } returns JobConfig.ConfigType.CHECK_CONNECTION_DESTINATION
            },
        )

      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)

      assertNull(result)
    }

    @Test
    fun `returns null when destination version is not found`() {
      every { actorDefinitionService.getActorDefinitionVersion(destinationVersionId) } throws ConfigNotFoundException("", "")
      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)
      assertNull(result)
    }

    @Test
    fun `returns null when destination version does not support data activation`() {
      val destinationVersion = ActorDefinitionVersion().withSupportsDataActivation(false)
      every { actorDefinitionService.getActorDefinitionVersion(destinationVersionId) } returns destinationVersion

      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)

      assertNull(result)
    }

    @Test
    fun `returns null when destination config has no objectStorageConfig`() {
      val destinationConfig =
        objectMapper.createObjectNode().apply {
          put("someOtherField", "value")
        }

      val destination = DestinationConnection().withConfiguration(destinationConfig)
      every { destinationService.getDestinationConnection(destinationId) } returns destination

      // When
      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)

      // Then
      assertNull(result)
    }

    @Test
    fun `returns null when objectStorageConfig has no storage_type`() {
      val objectStorageConfig =
        objectMapper.createObjectNode().apply {
          put("s3_bucket_name", "my-bucket")
          put("s3_bucket_region", "us-west-2")
          // Missing storage_type
        }

      val destination = createDestinationWithStorageConfig(objectStorageConfig)
      every { destinationService.getDestinationConnection(destinationId) } returns destination

      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)

      assertNull(result)
    }

    @Test
    fun `returns null when storage type is not supported`() {
      val objectStorageConfig =
        objectMapper.createObjectNode().apply {
          put("storage_type", "GCS") // Unsupported storage type
          put("bucket_name", "my-bucket")
        }

      val destination = createDestinationWithStorageConfig(objectStorageConfig)
      every { destinationService.getDestinationConnection(destinationId) } returns destination

      val result = connectorObjectStorageService.getRejectedRecordsForJob(connectionId, job, 100L)

      assertNull(result)
    }

    // Helper methods to create test data
    private fun createDestinationWithStorageConfig(objectStorageConfig: JsonNode): DestinationConnection {
      val destinationConfig =
        objectMapper.createObjectNode().apply {
          replace("object_storage_config", objectStorageConfig)
        }

      return DestinationConnection().withConfiguration(destinationConfig)
    }
  }
}
