/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.ConnectorRolloutRepository
import io.airbyte.data.services.impls.data.mappers.EntityConnectorRollout
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStrategyType
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class ConnectorRolloutServiceDataImplTest {
  private val connectorRolloutRepository = mockk<ConnectorRolloutRepository>()
  private val connectorRolloutService = ConnectorRolloutServiceDataImpl(connectorRolloutRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get rollout by id`() {
    val rolloutId = UUID.randomUUID()
    val expectedRollout = createMockConnectorRollout(rolloutId)

    every { connectorRolloutRepository.findById(rolloutId) } returns Optional.of(expectedRollout)

    val rollout = connectorRolloutService.getConnectorRollout(rolloutId)
    assertEquals(expectedRollout.toConfigModel(), rollout)

    verify { connectorRolloutRepository.findById(rolloutId) }
  }

  @Test
  fun `test get rollout by non-existent id throws`() {
    every { connectorRolloutRepository.findById(any()) } returns Optional.empty()

    assertThrows<ConfigNotFoundException> { connectorRolloutService.getConnectorRollout(UUID.randomUUID()) }

    verify { connectorRolloutRepository.findById(any()) }
  }

  @Test
  fun `test write new rollout`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())

    every { connectorRolloutRepository.existsById(rollout.id) } returns false
    every { connectorRolloutRepository.save(any()) } returns rollout

    val res = connectorRolloutService.writeConnectorRollout(rollout.toConfigModel())
    assertEquals(rollout.toConfigModel(), res)

    verify { connectorRolloutRepository.existsById(rollout.id) }
    verify { connectorRolloutRepository.save(any()) }
  }

  @Test
  fun `test update existing rollout`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())

    every { connectorRolloutRepository.existsById(rollout.id) } returns true
    every { connectorRolloutRepository.update(any()) } returns rollout

    val res = connectorRolloutService.writeConnectorRollout(rollout.toConfigModel())
    assertEquals(rollout.toConfigModel(), res)

    verify { connectorRolloutRepository.existsById(rollout.id) }
    verify { connectorRolloutRepository.update(any()) }
  }

  @Test
  fun `test list rollouts`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val rollout1 = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, releaseCandidateVersionId)
    val rollout2 = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, releaseCandidateVersionId)

    every {
      connectorRolloutRepository.findAllOrderByUpdatedAtDesc()
    } returns listOf(rollout1, rollout2)

    val res = connectorRolloutService.listConnectorRollouts()
    assertEquals(listOf(rollout1.toConfigModel(), rollout2.toConfigModel()), res)

    verify {
      connectorRolloutRepository.findAllOrderByUpdatedAtDesc()
    }
  }

  @Test
  fun `test list rollouts by actor definition id`() {
    val actorDefinitionId = UUID.randomUUID()
    val rollout1 = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, UUID.randomUUID())
    val rollout2 = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, UUID.randomUUID())

    every {
      connectorRolloutRepository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
      )
    } returns listOf(rollout1, rollout2)

    val res = connectorRolloutService.listConnectorRollouts(actorDefinitionId)
    assertEquals(listOf(rollout1.toConfigModel(), rollout2.toConfigModel()), res)

    verify {
      connectorRolloutRepository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
      )
    }
  }

  @Test
  fun `test list rollouts by actor definition id and version id`() {
    val actorDefinitionId = UUID.randomUUID()
    val releaseCandidateVersionId = UUID.randomUUID()
    val rollout1 = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, releaseCandidateVersionId)
    val rollout2 = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, releaseCandidateVersionId)

    every {
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )
    } returns listOf(rollout1, rollout2)

    val res = connectorRolloutService.listConnectorRollouts(actorDefinitionId, releaseCandidateVersionId)
    assertEquals(listOf(rollout1.toConfigModel(), rollout2.toConfigModel()), res)

    verify {
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      )
    }
  }

  @Test
  fun `should throw exception if rollout already exists for actor definition id and version id`() {
    val rollout = createMockConnectorRollout(UUID.randomUUID())
    every {
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        rollout.actorDefinitionId,
        rollout.releaseCandidateVersionId,
      )
    } returns listOf(rollout)

    val exception =
      assertThrows<RuntimeException> {
        connectorRolloutService.insertConnectorRollout(rollout.toConfigModel())
      }

    assertEquals(
      "A rollout in state initialized already exists " +
        "for actor definition id ${rollout.actorDefinitionId} " +
        "and version id ${rollout.releaseCandidateVersionId}",
      exception.message,
    )

    verify(exactly = 0) { connectorRolloutRepository.save(any()) }
  }

  @Test
  fun `should cancel existing rollout if incomplete rollout exists for actor definition id`() {
    val actorDefinitionId = UUID.randomUUID()

    val newRollout = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, UUID.randomUUID())
    val existingRollout = createMockConnectorRollout(UUID.randomUUID(), actorDefinitionId, UUID.randomUUID(), ConnectorRolloutStateType.in_progress)

    every {
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        newRollout.actorDefinitionId,
        newRollout.releaseCandidateVersionId,
      )
    } returns emptyList()

    every {
      connectorRolloutRepository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(
        newRollout.actorDefinitionId,
      )
    } returns listOf(existingRollout)

    every { connectorRolloutRepository.save(any()) } returns existingRollout

    connectorRolloutService.insertConnectorRollout(newRollout.toConfigModel())

    verify(exactly = 2) { connectorRolloutRepository.save(any()) }
  }

  @Test
  fun `should save connector rollout if no existing rollout conflicts`() {
    val newRollout = createMockConnectorRollout(UUID.randomUUID())

    every {
      connectorRolloutRepository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        newRollout.actorDefinitionId,
        newRollout.releaseCandidateVersionId,
      )
    } returns emptyList()

    every {
      connectorRolloutRepository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(newRollout.actorDefinitionId)
    } returns emptyList()

    every { connectorRolloutRepository.save(any()) } returns newRollout

    val result = connectorRolloutService.insertConnectorRollout(newRollout.toConfigModel())

    verify { connectorRolloutRepository.save(any()) }
    assertEquals(newRollout.toConfigModel(), result)
  }

  private fun createMockConnectorRollout(
    id: UUID,
    actorDefinitionId: UUID = UUID.randomUUID(),
    releaseCandidateVersionId: UUID = UUID.randomUUID(),
    state: ConnectorRolloutStateType = ConnectorRolloutStateType.initialized,
  ): EntityConnectorRollout =
    EntityConnectorRollout(
      id = id,
      actorDefinitionId = actorDefinitionId,
      releaseCandidateVersionId = releaseCandidateVersionId,
      initialVersionId = UUID.randomUUID(),
      state = state,
      initialRolloutPct = 10,
      finalTargetRolloutPct = 100,
      hasBreakingChanges = false,
      rolloutStrategy = ConnectorRolloutStrategyType.manual,
      maxStepWaitTimeMins = 60,
      createdAt = OffsetDateTime.now(),
      updatedAt = OffsetDateTime.now(),
      expiresAt = OffsetDateTime.now().plusDays(1),
    )
}
