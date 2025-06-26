/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.RefreshMode
import io.airbyte.commons.server.handlers.StreamRefreshesHandler.Companion.connectionStreamsToStreamDescriptors
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.RefreshConfig
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ConnectionEvent
import io.airbyte.persistence.job.JobPersistence
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class StreamRefreshesHandlerTest {
  private val connectionService: ConnectionService = mockk()
  private val streamRefreshesRepository: StreamRefreshesRepository = mockk(relaxed = true)
  private val eventRunner: EventRunner = mockk()
  private val actorDefinitionVersionHandler: ActorDefinitionVersionHandler = mockk()
  private val jobPersistence: JobPersistence = mockk()
  private val connectionTimelineEventService: ConnectionTimelineEventService = mockk()
  private val connectionTimelineEventHelper: ConnectionTimelineEventHelper = mockk()

  private val streamRefreshesHandler =
    StreamRefreshesHandler(
      connectionService,
      streamRefreshesRepository,
      eventRunner,
      actorDefinitionVersionHandler,
      connectionTimelineEventHelper,
      jobPersistence,
      connectionTimelineEventService,
    )

  private val connectionId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val connectionStream =
    listOf(
      ConnectionStream().streamName("name1").streamNamespace("namespace1"),
      ConnectionStream().streamName("name2"),
    )
  private val streamDescriptors =
    listOf(
      StreamDescriptor().withName("name1").withNamespace("namespace1"),
      StreamDescriptor().withName("name2"),
    )

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test that nothing is submitted if refreshes is not supported`() {
    mockSupportRefresh(false)

    val result = streamRefreshesHandler.createRefreshesForConnection(connectionId, RefreshMode.TRUNCATE, listOf())

    assertNull(result)

    verify(exactly = 0) { streamRefreshesRepository.saveAll(any<List<StreamRefresh>>()) }
    verify(exactly = 0) { eventRunner.startNewManualSync(connectionId) }
  }

  @Test
  fun `test that the refreshes entries are properly created`() {
    mockSupportRefresh(true)

    every { streamRefreshesRepository.saveAll(any<List<StreamRefresh>>()) } returns listOf()
    every { eventRunner.startNewManualSync(connectionId) } returns ManualOperationResult(jobId = 0L)
    every { connectionTimelineEventHelper.currentUserIdIfExist } returns UUID.randomUUID()
    every { connectionTimelineEventService.writeEvent(any(), any(), any()) } returns
      ConnectionTimelineEvent(
        connectionId = UUID.randomUUID(),
        eventType = ConnectionEvent.Type.REFRESH_STARTED.toString(),
      )
    every { jobPersistence.getJob(any()) } returns
      Job(
        0L,
        JobConfig.ConfigType.REFRESH,
        "scope_id",
        JobConfig().withRefresh(RefreshConfig()),
        listOf(),
        io.airbyte.config.JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
        true,
      )
    val result = streamRefreshesHandler.createRefreshesForConnection(connectionId, RefreshMode.TRUNCATE, connectionStream)

    assertNotNull(result)

    verifyOrder {
      streamRefreshesRepository.saveAll(any<List<StreamRefresh>>())
      eventRunner.startNewManualSync(connectionId)
    }
  }

  @Test
  fun `test that the refreshes entries are properly created for all the streams if the provided list is empty`() {
    mockSupportRefresh(true)

    every { streamRefreshesRepository.saveAll(any<List<StreamRefresh>>()) } returns listOf()
    every { eventRunner.startNewManualSync(connectionId) } returns ManualOperationResult(jobId = 0L)
    every { connectionTimelineEventHelper.currentUserIdIfExist } returns UUID.randomUUID()
    every { connectionTimelineEventService.writeEvent(any(), any(), any()) } returns
      ConnectionTimelineEvent(
        connectionId = UUID.randomUUID(),
        eventType = ConnectionEvent.Type.REFRESH_STARTED.toString(),
      )
    every { connectionService.getAllStreamsForConnection(connectionId) } returns streamDescriptors
    every { jobPersistence.getJob(any()) } returns
      Job(
        0L,
        JobConfig.ConfigType.REFRESH,
        "scope_id",
        JobConfig().withRefresh(RefreshConfig()),
        listOf(),
        io.airbyte.config.JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
        true,
      )
    val result = streamRefreshesHandler.createRefreshesForConnection(connectionId, RefreshMode.TRUNCATE, listOf())

    assertNotNull(result)

    verifyOrder {
      streamRefreshesRepository.saveAll(any<List<StreamRefresh>>())
      eventRunner.startNewManualSync(connectionId)
    }
  }

  @Test
  fun `test the conversion from connection stream to stream descriptors`() {
    val result = connectionStreamsToStreamDescriptors(connectionStream)

    assertEquals(streamDescriptors, result)
  }

  @Test
  fun `test delete`() {
    val connectionId: UUID = UUID.randomUUID()
    every { streamRefreshesRepository.deleteByConnectionId(connectionId) }.returns(Unit)
    streamRefreshesHandler.deleteRefreshesForConnection(connectionId)
    verify { streamRefreshesRepository.deleteByConnectionId(connectionId) }
  }

  fun mockSupportRefresh(supportRefresh: Boolean) {
    every { connectionService.getStandardSync(connectionId) } returns StandardSync().withDestinationId(destinationId)
    every { actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(DestinationIdRequestBody().destinationId(destinationId)) } returns
      ActorDefinitionVersionRead().supportsRefreshes(supportRefresh)
  }
}
