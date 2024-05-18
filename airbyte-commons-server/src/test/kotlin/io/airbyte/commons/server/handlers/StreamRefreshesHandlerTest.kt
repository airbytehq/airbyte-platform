package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.RefreshMode
import io.airbyte.commons.server.handlers.StreamRefreshesHandler.Companion.connectionStreamsToStreamDescriptors
import io.airbyte.commons.server.handlers.StreamRefreshesHandler.Companion.streamDescriptorsToStreamRefreshes
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType
import io.airbyte.featureflag.ActivateRefreshes
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.StreamDescriptor
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

internal class StreamRefreshesHandlerTest {
  private val connectionService: ConnectionService = mockk()
  private val streamRefreshesRepository: StreamRefreshesRepository = mockk()
  private val eventRunner: EventRunner = mockk()
  private val workspaceService: WorkspaceService = mockk()
  private val featureFlagClient: FeatureFlagClient = mockk()

  private val streamRefreshesHandler =
    StreamRefreshesHandler(
      connectionService,
      streamRefreshesRepository,
      eventRunner,
      workspaceService,
      featureFlagClient,
    )

  private val workspaceId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val ffContext =
    Multi(
      listOf(
        Workspace(workspaceId),
        Connection(connectionId),
      ),
    )
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
    every {
      workspaceService.getStandardWorkspaceFromConnection(connectionId, false)
    } returns StandardWorkspace().withWorkspaceId(workspaceId)
  }

  @Test
  fun `test that nothing is submitted if the flag is disabled`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, ffContext) } returns false

    val result = streamRefreshesHandler.createRefreshesForConnection(connectionId, RefreshMode.TRUNCATE, listOf())

    assertFalse(result)

    verify(exactly = 0) { streamRefreshesRepository.saveAll(any<List<StreamRefresh>>()) }
    verify(exactly = 0) { eventRunner.startNewManualSync(connectionId) }
  }

  @Test
  fun `test that the refreshes entries are properly created`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, ffContext) } returns true
    every { streamRefreshesRepository.saveAll(any<List<StreamRefresh>>()) } returns listOf()
    every { eventRunner.startNewManualSync(connectionId) } returns null

    val result = streamRefreshesHandler.createRefreshesForConnection(connectionId, RefreshMode.TRUNCATE, connectionStream)

    assertTrue(result)

    verifyOrder {
      streamRefreshesRepository.saveAll(any<List<StreamRefresh>>())
      eventRunner.startNewManualSync(connectionId)
    }
  }

  @Test
  fun `test that the refreshes entries are properly created for all the streams if the provided list is empty`() {
    every { featureFlagClient.boolVariation(ActivateRefreshes, ffContext) } returns true
    every { streamRefreshesRepository.saveAll(any<List<StreamRefresh>>()) } returns listOf()
    every { eventRunner.startNewManualSync(connectionId) } returns null
    every { connectionService.getAllStreamsForConnection(connectionId) } returns streamDescriptors

    val result = streamRefreshesHandler.createRefreshesForConnection(connectionId, RefreshMode.TRUNCATE, listOf())

    assertTrue(result)

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

  @ParameterizedTest
  @EnumSource(RefreshMode::class)
  fun `test the conversion from stream descriptors to stream refreshes`(refreshMode: RefreshMode) {
    val expectedRefreshType =
      when (refreshMode) {
        RefreshMode.TRUNCATE -> RefreshType.TRUNCATE
        RefreshMode.MERGE -> RefreshType.MERGE
      }

    val result = streamDescriptorsToStreamRefreshes(connectionId, refreshMode, streamDescriptors)

    assertEquals(2, result.size)
    result.stream().forEach {
      assertEquals(connectionId, it.connectionId)
      assertEquals(expectedRefreshType, it.refreshType)
      when (it.streamNamespace) {
        null -> assertEquals("name2", it.streamName)
        "namespace1" -> assertEquals("name1", it.streamName)
        else -> throw RuntimeException("Unexpected streamNamespace {${it.streamNamespace}}")
      }
    }
  }

  @Test
  fun `test delete`() {
    val connectionId: UUID = UUID.randomUUID()
    every { streamRefreshesRepository.deleteByConnectionId(connectionId) }.returns(Unit)
    streamRefreshesHandler.deleteRefreshesForConnection(connectionId)
    verify { streamRefreshesRepository.deleteByConnectionId(connectionId) }
  }
}
