/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.shared.ActorSyncJobInfo
import io.airbyte.connector.rollout.shared.RolloutActorFinder
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

internal class ConnectorRolloutHelperTest {
  private val connectorRolloutService = mockk<ConnectorRolloutService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val userPersistence = mockk<UserPersistence>()
  private val rolloutActorFinder = mockk<RolloutActorFinder>()
  private val connectorRolloutHelper =
    ConnectorRolloutHelper(
      connectorRolloutService,
      actorDefinitionService,
      userPersistence,
      rolloutActorFinder,
    )

  companion object {
    val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    val RELEASE_CANDIDATE_VERSION_ID: UUID = UUID.randomUUID()
  }

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test getActorSyncInfo`() {
    val rolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(rolloutId)
    val actorId = UUID.randomUUID()
    val nSucceeded = 1
    val nFailed = 2
    val nConnections = 5
    val actorSyncJobInfo = ActorSyncJobInfo(nSucceeded, nFailed, nConnections)

    every { connectorRolloutService.getConnectorRollout(rolloutId) } returns connectorRollout
    every { rolloutActorFinder.getSyncInfoForPinnedActors(connectorRollout) } returns mapOf(actorId to actorSyncJobInfo)

    val result = connectorRolloutHelper.getActorSyncInfo(rolloutId)

    assertEquals(1, result.size)
    assertEquals(actorId, result.values.first().actorId)
    assertEquals(nConnections, result.values.first().getNumConnections())
    assertEquals(nSucceeded, result.values.first().getNumSucceeded())
    assertEquals(nFailed, result.values.first().getNumFailed())
  }

  private fun createMockConnectorRollout(
    id: UUID,
    actorDefinitionId: UUID = ACTOR_DEFINITION_ID,
    releaseCandidateVersionId: UUID = RELEASE_CANDIDATE_VERSION_ID,
    rolloutStrategy: ConnectorEnumRolloutStrategy? = ConnectorEnumRolloutStrategy.MANUAL,
  ): ConnectorRollout =
    ConnectorRollout(
      id = id,
      actorDefinitionId = actorDefinitionId,
      releaseCandidateVersionId = releaseCandidateVersionId,
      initialVersionId = UUID.randomUUID(),
      state = ConnectorEnumRolloutState.INITIALIZED,
      initialRolloutPct = 10,
      finalTargetRolloutPct = 100,
      hasBreakingChanges = false,
      rolloutStrategy = rolloutStrategy,
      maxStepWaitTimeMins = 60,
      createdAt = OffsetDateTime.now().toEpochSecond(),
      updatedAt = OffsetDateTime.now().toEpochSecond(),
      expiresAt = OffsetDateTime.now().plusDays(1).toEpochSecond(),
      tag = null,
    )
}
