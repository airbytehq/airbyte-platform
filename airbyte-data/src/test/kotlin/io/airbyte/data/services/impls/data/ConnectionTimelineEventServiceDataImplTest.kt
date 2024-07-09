/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.config.FailureReason
import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.SyncFailedEvent
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
    val syncFailedEvent =
      SyncFailedEvent(
        jobId = 100L,
        startTimeEpochSeconds = 10L,
        endTimeEpochSeconds = 11L,
        bytesLoaded = 0L,
        recordsLoaded = 2L,
        attemptsCount = 5,
        failureReason = Optional.of(FailureReason()),
      )
    service.writeEvent(connectionId = connectionId, event = syncFailedEvent, userId = null)
    verify {
      repository.save(any())
    }
  }
}
