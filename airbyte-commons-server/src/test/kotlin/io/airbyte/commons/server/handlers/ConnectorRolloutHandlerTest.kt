package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectorRolloutCreateRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class ConnectorRolloutHandlerTest {
  private val connectorRolloutService = mockk<ConnectorRolloutService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val connectorRolloutHandler = ConnectorRolloutHandler(connectorRolloutService, actorDefinitionService)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test getConnectorRollout by id`() {
    val rolloutId = UUID.randomUUID()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns expectedRollout

    val rolloutRead = connectorRolloutHandler.getConnectorRollout(rolloutId)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), rolloutRead)

    verify { connectorRolloutService.getConnectorRollout(rolloutId) }
  }

  @Test
  fun `test insertConnectorRollout`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    val mockActorDefinitionVersion =
      ActorDefinitionVersion().apply {
        this.versionId = UUID.randomUUID()
      }
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.of(mockActorDefinitionVersion)
    every { connectorRolloutService.writeConnectorRollout(any()) } returns expectedRollout

    val insertedRolloutRead = connectorRolloutHandler.insertConnectorRollout(connectorRolloutCreate)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), insertedRolloutRead)

    verifyAll {
      connectorRolloutService.writeConnectorRollout(any())
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    }
  }

  @Test
  fun `test insertConnectorRolloutNoActorDefinitionVersionForInitialVersion`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.empty()
    every { connectorRolloutService.writeConnectorRollout(any()) } returns expectedRollout

    val insertedRolloutRead = connectorRolloutHandler.insertConnectorRollout(connectorRolloutCreate)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), insertedRolloutRead)

    verifyAll {
      connectorRolloutService.writeConnectorRollout(any())
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    }
  }

  @Test
  fun `test updateConnectorRollout`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    val mockActorDefinitionVersion =
      ActorDefinitionVersion().apply {
        this.versionId = UUID.randomUUID()
      }
    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.of(mockActorDefinitionVersion)
    every { connectorRolloutService.writeConnectorRollout(any()) } returns expectedRollout

    val updatedRolloutRead = connectorRolloutHandler.updateConnectorRollout(rolloutId, connectorRolloutCreate)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), updatedRolloutRead)

    verify {
      connectorRolloutService.writeConnectorRollout(any())
    }
  }

  @Test
  fun `test updateConnectorRolloutNoActorDefinitionVersionForInitialVersion`() {
    val rolloutId = UUID.randomUUID()
    val connectorRolloutCreate = createMockConnectorRolloutCreateRequestBody()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    every { actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any()) } returns Optional.empty()
    every { connectorRolloutService.writeConnectorRollout(any()) } returns expectedRollout

    val updatedRolloutRead = connectorRolloutHandler.updateConnectorRollout(rolloutId, connectorRolloutCreate)

    assertEquals(connectorRolloutHandler.buildConnectorRolloutRead(expectedRollout), updatedRolloutRead)

    verify {
      connectorRolloutService.writeConnectorRollout(any())
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(any())
    }
  }

  @Test
  fun `test listConnectorRollouts`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val dockerImageTag = "0.1"

    val mockActorDefinitionVersion =
      ActorDefinitionVersion().apply {
        this.actorDefinitionId = actorDefinitionId
        this.versionId = releaseCandidateVersionId
      }

    val expectedRollouts =
      listOf(
        createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, releaseCandidateVersionId),
        createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, releaseCandidateVersionId),
      )

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) } returns Optional.of(mockActorDefinitionVersion)
    every { connectorRolloutService.listConnectorRollouts(actorDefinitionId, releaseCandidateVersionId) } returns expectedRollouts

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(actorDefinitionId, dockerImageTag)

    assertEquals(expectedRollouts.map { connectorRolloutHandler.buildConnectorRolloutRead(it) }, rolloutReads)

    verify { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) }
    verify { connectorRolloutService.listConnectorRollouts(actorDefinitionId, releaseCandidateVersionId) }
  }

  @Test
  fun `test listConnectorRolloutsNoActorDefinitionVersion`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val dockerImageTag = "0.1"

    every { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) } returns Optional.empty()

    val rolloutReads = connectorRolloutHandler.listConnectorRollouts(actorDefinitionId, dockerImageTag)

    assertEquals(emptyList<ConnectorRolloutRead>(), rolloutReads)

    verify { actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag) }
    verify(exactly = 0) { connectorRolloutService.listConnectorRollouts(actorDefinitionId, releaseCandidateVersionId) }
  }

  private fun createMockConnectorRollout(
    id: UUID,
    actorDefinitionId: UUID = UUID.randomUUID(),
    releaseCandidateVersionId: UUID = UUID.randomUUID(),
  ): ConnectorRollout {
    return ConnectorRollout().apply {
      this.id = id
      this.actorDefinitionId = actorDefinitionId
      this.releaseCandidateVersionId = releaseCandidateVersionId
      this.state = ConnectorEnumRolloutState.INITIALIZED
      this.initialRolloutPct = 10L
      this.finalTargetRolloutPct = 100L
      this.hasBreakingChanges = false
      this.rolloutStrategy = ConnectorEnumRolloutStrategy.MANUAL
      this.maxStepWaitTimeMins = 60L
      this.createdAt = OffsetDateTime.now().toEpochSecond()
      this.updatedAt = OffsetDateTime.now().toEpochSecond()
      this.expiresAt = OffsetDateTime.now().plusDays(1).toEpochSecond()
    }
  }

  private fun createMockConnectorRolloutCreateRequestBody(): ConnectorRolloutCreateRequestBody {
    return ConnectorRolloutCreateRequestBody()
      .actorDefinitionId(UUID.randomUUID())
      .releaseCandidateVersionId(UUID.randomUUID())
      .initialRolloutPct(10)
      .finalTargetRolloutPct(100)
      .hasBreakingChanges(false)
      .rolloutStrategy(ConnectorRolloutStrategy.MANUAL)
      .maxStepWaitTimeMins(60)
      .expiresAt(OffsetDateTime.now().plusDays(1))
  }
}
