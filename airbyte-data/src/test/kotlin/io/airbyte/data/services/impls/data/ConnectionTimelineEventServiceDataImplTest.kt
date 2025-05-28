/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.config.FailureReason
import io.airbyte.config.JobConfig
import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.FailedEvent
import io.airbyte.data.services.shared.SchemaChangeAutoPropagationEvent
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class ConnectionTimelineEventServiceDataImplTest {
  private lateinit var repository: ConnectionTimelineEventRepository
  private lateinit var service: ConnectionTimelineEventService

  @BeforeEach
  internal fun setUp() {
    repository = mockk()
    service =
      ConnectionTimelineEventServiceDataImpl(
        repository = repository,
        mapper = MoreMappers.initMapper(),
      )
  }

  @Test
  internal fun `write sync failed event`() {
    val connectionId = UUID.randomUUID()
    every {
      repository.save(any())
    } returns ConnectionTimelineEvent(connectionId = connectionId, eventType = "")
    every {
      repository.findDuplicateEvent(connectionId, any(), any(), any(), any())
    } returns null

    val syncFailedEvent =
      FailedEvent(
        jobId = 100L,
        startTimeEpochSeconds = 10L,
        endTimeEpochSeconds = 11L,
        bytesLoaded = 0L,
        recordsLoaded = 2L,
        attemptsCount = 5,
        jobType = JobConfig.ConfigType.SYNC.name,
        statusType = JobStatus.failed.name.uppercase(),
        failureReason = Optional.of(FailureReason()),
      )
    val writtenEvent = service.writeEvent(connectionId = connectionId, event = syncFailedEvent, userId = null)
    verify {
      repository.save(any())
    }
  }

  @Test
  internal fun `Write schema change event`() {
    val connectionId = UUID.randomUUID()
    every {
      repository.save(any())
    } returns ConnectionTimelineEvent(connectionId = connectionId, eventType = "")
    every {
      repository.findDuplicateEvent(connectionId, any(), any(), any(), any())
    } returns null

    val schemaChangeEvent =
      SchemaChangeAutoPropagationEvent(
        catalogDiff = CatalogDiff().addTransformsItem(StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)),
      )
    val writtenEvent = service.writeEvent(connectionId = connectionId, event = schemaChangeEvent, userId = null)
    verify {
      repository.save(any())
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  internal fun `writeEvent should save new event if no duplicate exists`(withCreatedAt: Boolean) {
    val schemaChangeEvent =
      SchemaChangeAutoPropagationEvent(
        catalogDiff = CatalogDiff().addTransformsItem(StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)),
      )

    val connectionId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val eventSummary = """{"test":"value"}"""
    val eventType = schemaChangeEvent.getEventType().toString()
    val createdAt = if (withCreatedAt) OffsetDateTime.now() else null

    val timelineEvent =
      ConnectionTimelineEvent(
        id = null,
        connectionId = connectionId,
        userId = userId,
        eventType = eventType,
        summary = eventSummary,
        createdAt = null,
      )

    every {
      repository.findDuplicateEvent(connectionId, userId, eventType, any(), createdAt)
    } returns null

    every {
      repository.save(any())
    } returns timelineEvent.copy(id = UUID.randomUUID())

    ConnectionTimelineEventServiceDataImpl(
      repository = repository,
      mapper = MoreMappers.initMapper(),
    ).writeEvent(connectionId, schemaChangeEvent, userId, createdAt)

    verify(exactly = 1) { repository.findDuplicateEvent(connectionId, userId, eventType, any(), createdAt) }
    verify(exactly = 1) { repository.save(any()) }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  internal fun `writeEvent should not save if duplicate exists`(withCreatedAt: Boolean) {
    val schemaChangeEvent =
      SchemaChangeAutoPropagationEvent(
        catalogDiff = CatalogDiff().addTransformsItem(StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)),
      )

    val connectionId = UUID.randomUUID()
    val userId = UUID.randomUUID()
    val eventSummary = """{"test":"value"}"""
    val eventType = schemaChangeEvent.getEventType().toString()
    val createdAt = if (withCreatedAt) OffsetDateTime.now() else null

    val existingEvent =
      ConnectionTimelineEvent(
        id = UUID.randomUUID(),
        connectionId = connectionId,
        userId = userId,
        eventType = eventType,
        summary = eventSummary,
        createdAt = null,
      )

    every {
      repository.findDuplicateEvent(connectionId, userId, eventType, any(), createdAt)
    } returns existingEvent

    ConnectionTimelineEventServiceDataImpl(
      repository = repository,
      mapper = MoreMappers.initMapper(),
    ).writeEvent(connectionId, schemaChangeEvent, userId, createdAt)

    verify(exactly = 1) { repository.findDuplicateEvent(connectionId, userId, eventType, any(), createdAt) }
    verify(exactly = 0) { repository.save(any()) }
  }
}
