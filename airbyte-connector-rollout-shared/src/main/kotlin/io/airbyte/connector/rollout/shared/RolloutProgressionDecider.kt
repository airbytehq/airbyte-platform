/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.connector.rollout.shared.Constants.DEFAULT_PERCENTAGE_OF_ACTORS_WITH_COMPLETED_SYNCS_REQUIRED
import io.airbyte.connector.rollout.shared.Constants.DEFAULT_SUCCESS_THRESHOLD_PERCENTAGE
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID

enum class Decision {
  RELEASE,
  ROLLBACK,
  INSUFFICIENT_DATA,
  PAUSE,
}

private val logger = KotlinLogging.logger {}

class RolloutProgressionDecider {
  fun decide(connectorRolloutOutput: ConnectorRolloutOutput): Decision {
    if (connectorRolloutOutput.actorSelectionInfo == null || connectorRolloutOutput.actorSyncs == null) {
      throw IllegalStateException(
        "RolloutProgressionDecider.decide: actorSelectionInfo and/or actorSyncs are missing from connectorRolloutOutput. " +
          "connectorRolloutOutput: $connectorRolloutOutput",
      )
    }
    val finalPercentageToPin = connectorRolloutOutput.finalTargetRolloutPct ?: 0
    val actorSelectionInfo = connectorRolloutOutput.actorSelectionInfo!!
    val nActorsEligibleOrAlreadyPinned = actorSelectionInfo.getNumActorsEligibleOrAlreadyPinned()
    val nActorsPinned = actorSelectionInfo.getNumPinnedToConnectorRollout()
    val actorSyncs = connectorRolloutOutput.actorSyncs!!

    val nFailedSyncs = actorSyncs.values.sumOf { it.getNumFailed() }
    if (nFailedSyncs > 0) {
      // If any syncs have failed, we pause the rollout so the dev can decide the next steps
      // For now, they will have to manually pin & monitor if they want to proceed with the rollout
      // TODO: in the future, we will fail the rollout
      val decision = Decision.PAUSE
      logger.info { "RolloutProgressionDecider.decide: nFailedSyncs=$nFailedSyncs decision=$decision" }
      return decision
    }

    // If no syncs have failed but we haven't yet pinned enough actors, we wait for more data or roll out to more
    if (!hasEnoughPinned(nActorsPinned, nActorsEligibleOrAlreadyPinned, finalPercentageToPin)) {
      val decision = Decision.INSUFFICIENT_DATA
      logger.info {
        "RolloutProgressionDecider.decide: Not enough actors have been pinned. " +
          "nActorsPinned=$nActorsPinned " +
          "nActorsEligibleOrAlreadyPinned=$nActorsEligibleOrAlreadyPinned " +
          "finalPercentageToPin=$finalPercentageToPin " +
          "decision=$decision"
      }
      return decision
    }

    // If no syncs have failed but not enough syncs have finished, we wait for more data
    if (!hasEnoughFinishedSyncs(actorSyncs, nActorsPinned, DEFAULT_PERCENTAGE_OF_ACTORS_WITH_COMPLETED_SYNCS_REQUIRED)) {
      val decision = Decision.INSUFFICIENT_DATA
      logger.info {
        "RolloutProgressionDecider.decide: Not enough actors have finished syncs. " +
          "nActorsWithSyncs=${actorSyncs.keys.size} " +
          "actorSyncs=$actorSyncs " +
          "nActorsPinned=$nActorsPinned " +
          "decision=$decision"
      }
      return decision
    }

    // We have enough data to conclusively decide that the sync is successful
    return if (isSuccessful(actorSyncs, DEFAULT_SUCCESS_THRESHOLD_PERCENTAGE)) {
      val decision = Decision.RELEASE
      logger.info { "RolloutProgressionDecider.decide: rollout is successful. actorSyncs=$actorSyncs decision=$decision" }
      decision
    } else {
      val decision = Decision.PAUSE
      logger.info {
        "RolloutProgressionDecider.decide: rollout was not successful but appears to have sufficient finished syncs. This is unexpected. " +
          "actorSyncs=$actorSyncs " +
          "decision=$decision"
      }
      decision
    }
  }

  @VisibleForTesting
  internal fun hasEnoughPinned(
    nActorsPinned: Int,
    nActorsEligibleOrAlreadyPinned: Int,
    finalPercentageToPin: Int,
  ): Boolean = (nActorsPinned.toFloat() / nActorsEligibleOrAlreadyPinned) * 100 >= finalPercentageToPin

  @VisibleForTesting
  internal fun hasEnoughFinishedSyncs(
    actorSyncs: Map<UUID, ConnectorRolloutActorSyncInfo>,
    nActorsPinned: Int,
    percentageRequired: Int,
  ): Boolean {
    val actorsWithCompletedSyncs =
      actorSyncs
        .filter { (_, syncInfo) ->
          syncInfo.getNumSucceeded() >= 1 || syncInfo.getNumFailed() >= 1
        }.count()
    val percentageActorsWithCompletedSyncs = actorsWithCompletedSyncs / nActorsPinned * 100
    logger.info {
      "RolloutProgressionDecider.hasEnoughFinishedSyncs " +
        "nActorsPinned=$nActorsPinned percentageRequired=$percentageRequired actorSyncs.size=${actorSyncs.size} " +
        "percentageActorsWithCompletedSyncs=$percentageActorsWithCompletedSyncs actorsWithCompletedSyncs=$actorsWithCompletedSyncs " +
        "actorSyncs=$actorSyncs"
    }
    return percentageActorsWithCompletedSyncs >= percentageRequired
  }

  @VisibleForTesting
  internal fun isSuccessful(
    actorSyncs: Map<UUID, ConnectorRolloutActorSyncInfo>,
    thresholdPercentage: Int,
  ): Boolean {
    val nSuccessfulSyncs = actorSyncs.values.sumOf { it.getNumSucceeded() }
    val nFailedSyncs = actorSyncs.values.sumOf { it.getNumFailed() }
    logger.info {
      "RolloutProgressionDecider.isSuccessful " +
        "nSuccessfulSyncs=$nSuccessfulSyncs nFailedSyncs=$nFailedSyncs thresholdPercentage=$thresholdPercentage actorSyncs=$actorSyncs"
    }
    return (nSuccessfulSyncs.toFloat() / (nSuccessfulSyncs + nFailedSyncs)) * 100 >= thresholdPercentage
  }
}
