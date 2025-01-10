package io.airbyte.connector.rollout.shared

import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class RolloutProgressionDeciderTest {
  @Test
  fun `decide throws exception when actorSelectionInfo or actorSyncs is null`() {
    val rolloutOutputMissingActorSelectionInfo = createConnectorRolloutOutput(actorSyncs = emptyMap())
    val rolloutOutputMissingActorSyncs = createConnectorRolloutOutput(actorSelectionInfo = mockActorSelectionInfo(1, 1))

    assertThrows(IllegalStateException::class.java) {
      RolloutProgressionDecider().decide(rolloutOutputMissingActorSelectionInfo)
    }

    assertThrows(IllegalStateException::class.java) {
      RolloutProgressionDecider().decide(rolloutOutputMissingActorSyncs)
    }
  }

  @Test
  fun `decide returns PAUSE when there are failed syncs`() {
    val actorSyncs =
      mapOf(
        UUID.randomUUID() to createActorSyncInfo(nSucceeded = 1, nFailed = 1, nConnections = 2),
      )
    val rolloutOutput =
      createConnectorRolloutOutput(
        actorSelectionInfo = mockActorSelectionInfo(),
        actorSyncs = actorSyncs,
      )

    assertEquals(Decision.PAUSE, RolloutProgressionDecider().decide(rolloutOutput))
  }

  @Test
  fun `decide returns INSUFFICIENT_DATA when not enough actors are pinned`() {
    val actorSelectionInfo =
      mockActorSelectionInfo(
        nActorsEligibleOrAlreadyPinned = 10,
        nPinnedToConnectorRollout = 0,
      )
    val rolloutOutput =
      createConnectorRolloutOutput(
        actorSelectionInfo = actorSelectionInfo,
        actorSyncs = emptyMap(),
        finalTargetRolloutPct = 50,
      )

    assertEquals(Decision.INSUFFICIENT_DATA, RolloutProgressionDecider().decide(rolloutOutput))
  }

  @Test
  fun `decide returns INSUFFICIENT_DATA when not enough syncs have finished`() {
    val actorSyncs =
      mapOf(
        UUID.randomUUID() to createActorSyncInfo(nSucceeded = 0, nFailed = 0, nConnections = 1),
      )
    val actorSelectionInfo =
      mockActorSelectionInfo(
        nActorsEligibleOrAlreadyPinned = 10,
        nPinnedToConnectorRollout = 10,
      )
    val rolloutOutput =
      createConnectorRolloutOutput(
        actorSelectionInfo = actorSelectionInfo,
        actorSyncs = actorSyncs,
        finalTargetRolloutPct = 50,
      )

    val decision = RolloutProgressionDecider().decide(rolloutOutput)
    assertEquals(Decision.INSUFFICIENT_DATA, decision)
  }

  @Test
  fun `decide returns RELEASE when rollout is successful`() {
    val actorSyncs =
      mapOf(
        UUID.randomUUID() to createActorSyncInfo(nSucceeded = 5, nFailed = 0, nConnections = 5),
        UUID.randomUUID() to createActorSyncInfo(nSucceeded = 3, nFailed = 0, nConnections = 3),
      )
    val actorSelectionInfo =
      mockActorSelectionInfo(
        nActorsEligibleOrAlreadyPinned = 2,
        nPinnedToConnectorRollout = 2,
      )
    val rolloutOutput =
      createConnectorRolloutOutput(
        actorSelectionInfo = actorSelectionInfo,
        actorSyncs = actorSyncs,
        finalTargetRolloutPct = 50,
      )

    assertEquals(Decision.RELEASE, RolloutProgressionDecider().decide(rolloutOutput))
  }

  @Test
  fun `decide returns PAUSE when rollout has sufficient finished syncs but is not successful`() {
    val actorSyncs =
      mapOf(
        UUID.randomUUID() to createActorSyncInfo(nSucceeded = 2, nFailed = 8, nConnections = 10),
        UUID.randomUUID() to createActorSyncInfo(nSucceeded = 1, nFailed = 4, nConnections = 5),
      )
    val actorSelectionInfo =
      mockActorSelectionInfo(
        nActorsEligibleOrAlreadyPinned = 10,
        nPinnedToConnectorRollout = 5,
      )
    val rolloutOutput =
      createConnectorRolloutOutput(
        actorSelectionInfo = actorSelectionInfo,
        actorSyncs = actorSyncs,
        finalTargetRolloutPct = 50,
      )

    val decision = RolloutProgressionDecider().decide(rolloutOutput)
    assertEquals(Decision.PAUSE, decision)
  }

  @Test
  fun `hasEnoughPinned returns false when percentage is less than finalPercentageToPin`() {
    assertFalse(RolloutProgressionDecider().hasEnoughPinned(nActorsPinned = 1, nActorsEligibleOrAlreadyPinned = 2, finalPercentageToPin = 75))
  }

  @Test
  fun `hasEnoughPinned returns true when percentage equals finalPercentageToPin`() {
    assertTrue(RolloutProgressionDecider().hasEnoughPinned(nActorsPinned = 1, nActorsEligibleOrAlreadyPinned = 2, finalPercentageToPin = 50))
  }

  @Test
  fun `hasEnoughPinned returns true when percentage exceeds finalPercentageToPin`() {
    assertTrue(RolloutProgressionDecider().hasEnoughPinned(nActorsPinned = 1, nActorsEligibleOrAlreadyPinned = 1, finalPercentageToPin = 50))
  }

  @Test
  fun `hasEnoughFinishedSyncs returns true when percentage of completed syncs meets threshold`() {
    val actorSyncs =
      mapOf(
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(1)
            .numFailed(0)
            .numConnections(1),
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(1)
            .numFailed(1)
            .numConnections(2),
      )
    assertTrue(
      RolloutProgressionDecider().hasEnoughFinishedSyncs(
        actorSyncs = actorSyncs,
        nActorsPinned = 2,
        percentageRequired = 50,
      ),
    )
  }

  @Test
  fun `hasEnoughFinishedSyncs returns false when percentage of actors with syncs does not meet threshold`() {
    val actorSyncs =
      mapOf(
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(1)
            .numFailed(0)
            .numConnections(1),
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(1)
            .numFailed(1)
            .numConnections(2),
      )
    assertFalse(
      RolloutProgressionDecider().hasEnoughFinishedSyncs(
        actorSyncs = actorSyncs,
        nActorsPinned = 3,
        percentageRequired = 100,
      ),
    )
  }

  @Test
  fun `hasEnoughFinishedSyncs returns false when percentage of completed syncs does not meet threshold`() {
    val actorSyncs =
      mapOf(
        // the first actor has syncs listed but not completed
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(0)
            .numFailed(0)
            .numConnections(1),
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(1)
            .numFailed(1)
            .numConnections(2),
      )
    assertFalse(
      RolloutProgressionDecider().hasEnoughFinishedSyncs(
        actorSyncs = actorSyncs,
        nActorsPinned = 2,
        percentageRequired = 50,
      ),
    )
  }

  @Test
  fun `isSuccessful returns true when success threshold is met`() {
    // all successful
    assertTrue(
      RolloutProgressionDecider().isSuccessful(
        mapOf(
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(0)
              .numConnections(1),
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(0)
              .numConnections(1),
        ),
        50,
      ),
    )

    // mix of success & failure
    assertTrue(
      RolloutProgressionDecider().isSuccessful(
        mapOf(
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(0)
              .numConnections(1),
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(1)
              .numConnections(2),
        ),
        50,
      ),
    )

    // the first actor has syncs listed but not completed
    assertTrue(
      RolloutProgressionDecider().isSuccessful(
        mapOf(
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(0)
              .numFailed(0)
              .numConnections(1),
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(1)
              .numConnections(2),
        ),
        50,
      ),
    )
  }

  @Test
  fun `isSuccessful returns false when success threshold is not met`() {
    // all failed
    assertFalse(
      RolloutProgressionDecider().isSuccessful(
        mapOf(
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(0)
              .numFailed(1)
              .numConnections(1),
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(0)
              .numFailed(0)
              .numConnections(1),
        ),
        50,
      ),
    )

    // mix of success & failure
    assertFalse(
      RolloutProgressionDecider().isSuccessful(
        mapOf(
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(0)
              .numConnections(1),
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(1)
              .numFailed(1)
              .numConnections(2),
        ),
        90,
      ),
    )

    // actors have syncs listed but not completed
    assertFalse(
      RolloutProgressionDecider().isSuccessful(
        mapOf(
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(0)
              .numFailed(0)
              .numConnections(1),
          UUID.randomUUID() to
            ConnectorRolloutActorSyncInfo()
              .numSucceeded(0)
              .numFailed(0)
              .numConnections(2),
        ),
        90,
      ),
    )
  }

  private fun createConnectorRolloutOutput(
    actorSelectionInfo: ConnectorRolloutActorSelectionInfo? = null,
    actorSyncs: Map<UUID, ConnectorRolloutActorSyncInfo>? = null,
    finalTargetRolloutPct: Int? = null,
  ): ConnectorRolloutOutput {
    return ConnectorRolloutOutput(
      state = ConnectorEnumRolloutState.IN_PROGRESS,
      finalTargetRolloutPct = finalTargetRolloutPct,
      actorSelectionInfo = actorSelectionInfo,
      actorSyncs = actorSyncs,
    )
  }

  private fun createActorSyncInfo(
    nSucceeded: Int,
    nFailed: Int,
    nConnections: Int,
  ): ConnectorRolloutActorSyncInfo {
    return ConnectorRolloutActorSyncInfo()
      .numSucceeded(nSucceeded)
      .numFailed(nFailed)
      .numConnections(nConnections)
  }

  private fun mockActorSelectionInfo(
    nActorsEligibleOrAlreadyPinned: Int = 1,
    nPinnedToConnectorRollout: Int = 1,
  ): ConnectorRolloutActorSelectionInfo {
    return ConnectorRolloutActorSelectionInfo()
      .numPinnedToConnectorRollout(nPinnedToConnectorRollout)
      .numActorsEligibleOrAlreadyPinned(nActorsEligibleOrAlreadyPinned)
  }
}
