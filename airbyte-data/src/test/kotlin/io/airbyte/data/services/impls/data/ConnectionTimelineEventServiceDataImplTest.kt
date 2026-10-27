/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamTransform
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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
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
  fun `ownership lookup preserves saved parent absence and failures without reading full event`() {
    val eventId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    every { repository.findConnectionIdByEventId(eventId) } returns Optional.of(connectionId)
    assertEquals(connectionId, service.findConnectionIdForEvent(eventId).get())

    every { repository.findConnectionIdByEventId(eventId) } returns Optional.empty()
    assertTrue(service.findConnectionIdForEvent(eventId).isEmpty)

    val failure = IOException("event ownership unavailable")
    every { repository.findConnectionIdByEventId(eventId) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.findConnectionIdForEvent(eventId) })
    verify(exactly = 0) { repository.findById(any()) }
  }

  @Test
  internal fun `write sync failed event`() {
    val connectionId = UUID.randomUUID()
    every {
      repository.save(any())
    } returns ConnectionTimelineEvent(connectionId = connectionId, eventType = "")
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
    val schemaChangeEvent =
      SchemaChangeAutoPropagationEvent(
        catalogDiff =
          CatalogDiff(
            transforms =
              listOf(
                StreamTransform(transformType = StreamTransform.TransformType.ADD_STREAM, streamDescriptor = StreamDescriptor(name = "stream")),
              ),
          ),
      )
    val writtenEvent = service.writeEvent(connectionId = connectionId, event = schemaChangeEvent, userId = null)
    verify {
      repository.save(any())
    }
  }
}
