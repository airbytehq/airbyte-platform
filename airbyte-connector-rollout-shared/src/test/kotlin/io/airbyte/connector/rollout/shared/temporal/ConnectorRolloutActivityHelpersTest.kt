/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.api.client.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.client.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.ConnectorRolloutStrategy
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class ConnectorRolloutActivityHelpersTest {
  @Test
  fun `test mapToConnectorRollout maps fields correctly`() {
    val rolloutId = UUID.randomUUID()
    val actorDefId = UUID.randomUUID()
    val rcVersionId = UUID.randomUUID()
    val initialVersionId = UUID.randomUUID()
    val actorId = UUID.randomUUID()

    val now = Instant.now()

    val rolloutRead =
      ConnectorRolloutRead(
        rolloutId,
        actorDefId,
        rcVersionId,
        ConnectorRolloutState.IN_PROGRESS,
        "airbyte/source-faker",
        "0.1",
        "workflow-123",
        initialVersionId,
        10,
        30,
        100,
        false,
        60,
        "updatedBy",
        now.atOffset(ZoneOffset.UTC),
        now.atOffset(ZoneOffset.UTC),
        null,
        null,
        "error msg",
        "failed reason",
        "paused reason",
        ConnectorRolloutStrategy.AUTOMATED,
        ConnectorRolloutActorSelectionInfo(10, 3, 7),
        mapOf(
          actorId.toString() to
            ConnectorRolloutActorSyncInfo(
              actorId,
              1,
              2,
              3,
            ),
        ),
      )

    val result: ConnectorRolloutOutput = ConnectorRolloutActivityHelpers.mapToConnectorRollout(rolloutRead)

    assertEquals(rolloutId, result.id)
    assertEquals("workflow-123", result.workflowRunId)
    assertEquals(actorDefId, result.actorDefinitionId)
    assertEquals(rcVersionId, result.releaseCandidateVersionId)
    assertEquals(initialVersionId, result.initialVersionId)
    assertEquals(ConnectorRolloutState.IN_PROGRESS.name, result.state.name)
    assertEquals(10, result.initialRolloutPct)
    assertEquals(30, result.currentTargetRolloutPct)
    assertEquals(100, result.finalTargetRolloutPct)
    assertEquals(false, result.hasBreakingChanges)
    assertEquals(ConnectorRolloutStrategy.AUTOMATED.name, result.rolloutStrategy!!.name)
    assertEquals(60, result.maxStepWaitTimeMins)
    assertEquals("updatedBy", result.updatedBy)
    assertEquals(now.atOffset(ZoneOffset.UTC), result.createdAt)
    assertEquals(now.atOffset(ZoneOffset.UTC), result.updatedAt)
    assertEquals(null, result.completedAt)
    assertEquals(null, result.expiresAt)
    assertEquals("error msg", result.errorMsg)
    assertEquals("failed reason", result.failedReason)
    assertEquals("paused reason", result.pausedReason)

    val actorInfo = result.actorSelectionInfo!!
    assertEquals(10, actorInfo.numActors)
    assertEquals(7, actorInfo.numPinnedToConnectorRollout)
    assertEquals(3, actorInfo.numActorsEligibleOrAlreadyPinned)

    val syncInfo = result.actorSyncs!![actorId]!!
    assertEquals(actorId, syncInfo.actorId)
    assertEquals(1, syncInfo.numSucceeded)
    assertEquals(2, syncInfo.numFailed)
    assertEquals(3, syncInfo.numConnections)
  }
}
