/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStateTerminal
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.api.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutInvalidRequestProblem
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutMaximumRolloutPercentageReachedProblem
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutNotEnoughActorsProblem
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.shared.ActorSelectionInfo
import io.airbyte.connector.rollout.shared.Constants.DEFAULT_MAX_ROLLOUT_PERCENTAGE
import io.airbyte.connector.rollout.shared.RolloutActorFinder
import io.airbyte.data.exceptions.InvalidRequestException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.math.ceil
import kotlin.math.min

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class ConnectorRolloutHandler
  @Inject
  constructor(
    private val connectorRolloutService: ConnectorRolloutService,
    private val actorDefinitionService: ActorDefinitionService,
    private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
    private val userPersistence: UserPersistence,
    private val rolloutActorFinder: RolloutActorFinder,
    private val connectorRolloutHelper: ConnectorRolloutHelper,
  ) {
    @VisibleForTesting
    open fun validateRolloutActorDefinitionId(
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
    ) {
      val actorDefinitionVersion =
        actorDefinitionService
          .getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
          .orElseThrow {
            throw ConnectorRolloutInvalidRequestProblem(
              ProblemMessageData().message(
                "Actor definition version not found for actor definition id: $actorDefinitionId and docker image tag: $dockerImageTag",
              ),
            )
          }

      if (actorDefinitionVersion.dockerRepository != dockerRepository || actorDefinitionVersion.dockerImageTag != dockerImageTag) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Actor definition ID does not match docker repository: $dockerRepository and docker image tag: $dockerImageTag",
          ),
        )
      }
    }

    @VisibleForTesting
    open fun getAndValidateStartRequest(connectorRolloutStart: ConnectorRolloutStartRequestBody): ConnectorRollout {
      // We expect to hit this code path under 2 different circumstances:
      // 1. When a rollout is being started for the first time
      // 2. When a rollout's Temporal workflow is being reset, e.g. for a bug fix.
      // In case 1, the rollout will be in INITIALIZED state, and we'll change the state to WORKFLOW_STARTED.
      // In case 2, the rollout may be in any state, and we only want to change it to WORKFLOW_STARTED if it was INITIALIZED.
      // However, in case 2 the workflow will have a new run ID, so we still want to update that.
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutStart.id)
      if (connectorRollout.state == ConnectorEnumRolloutState.INITIALIZED) {
        connectorRollout.state = ConnectorEnumRolloutState.WORKFLOW_STARTED
        connectorRollout.rolloutStrategy = ConnectorEnumRolloutStrategy.fromValue(connectorRolloutStart.rolloutStrategy.toString())
      }
      // Always update the workflow run ID if provided; if the workflow was restarted it will have changed
      connectorRollout.workflowRunId = connectorRolloutStart.workflowRunId
      // Also include the version ID, for cases where the rollout wasn't automatically added to the rollouts table (i.e. for testing)
      connectorRollout.initialVersionId = connectorRollout.initialVersionId
      return connectorRollout
    }

    @VisibleForTesting
    open fun getAndRollOutConnectorRollout(connectorRolloutRequest: ConnectorRolloutRequestBody): ConnectorRollout {
      var connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutRequest.id)
      val validStates =
        setOf(
          ConnectorEnumRolloutState.INITIALIZED,
          ConnectorEnumRolloutState.WORKFLOW_STARTED,
          ConnectorEnumRolloutState.IN_PROGRESS,
          ConnectorEnumRolloutState.PAUSED,
        )
      if (connectorRollout.state !in validStates) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Connector rollout must be in $validStates state to update the rollout, but was in state " +
              connectorRollout.state.toString(),
          ),
        )
      }
      if (connectorRolloutRequest.actorIds == null && connectorRolloutRequest.targetPercentage == null) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "ActorIds or targetPercentage must be provided, but neither were found.",
          ),
        )
      }
      if (connectorRolloutRequest.actorIds != null) {
        try {
          actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(
            connectorRolloutRequest.actorIds.toSet(),
            connectorRollout.actorDefinitionId,
            connectorRollout.releaseCandidateVersionId,
            connectorRollout.id,
          )
        } catch (e: InvalidRequestException) {
          throw ConnectorRolloutInvalidRequestProblem(
            ProblemMessageData().message("Failed to create release candidate pins for actors: ${e.message}"),
          )
        }
        connectorRollout.state = ConnectorEnumRolloutState.IN_PROGRESS
        connectorRollout.rolloutStrategy = ConnectorEnumRolloutStrategy.fromValue(connectorRolloutRequest.rolloutStrategy.toString())
        connectorRollout.updatedAt = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond()
      }
      if (connectorRolloutRequest.targetPercentage != null) {
        connectorRollout = pinByPercentage(connectorRollout, connectorRolloutRequest.targetPercentage!!, connectorRolloutRequest.rolloutStrategy!!)
      }

      // get current percentage pinned
      connectorRollout.currentTargetRolloutPct = getPercentagePinned(getActorSelectionInfo(connectorRollout, 0))
      return connectorRollout
    }

    fun getPercentagePinned(actorSelectionInfo: ActorSelectionInfo): Int {
      logger.info {
        "getPercentagePinned actorSelectionInfo=$actorSelectionInfo percentagePinned=${ceil(
          (actorSelectionInfo.nPreviouslyPinned + actorSelectionInfo.nNewPinned) / actorSelectionInfo.nActorsEligibleOrAlreadyPinned.toDouble(),
        )}"
      }
      return ceil(
        (100 * actorSelectionInfo.nPreviouslyPinned + actorSelectionInfo.nNewPinned) / actorSelectionInfo.nActorsEligibleOrAlreadyPinned.toDouble(),
      ).toInt()
    }

    open fun pinByPercentage(
      connectorRollout: ConnectorRollout,
      targetPercentage: Int,
      rolloutStrategy: ConnectorRolloutStrategy,
    ): ConnectorRollout {
      val percentageAlreadyPinned = getPercentagePinned(getActorSelectionInfo(connectorRollout, 0))

      val actualPercentageToPin =
        getValidPercentageToPin(
          connectorRollout,
          targetPercentage,
          rolloutStrategy,
          percentageAlreadyPinned,
        )

      val actorSelectionInfo = getActorSelectionInfo(connectorRollout, actualPercentageToPin)
      if (actorSelectionInfo.actorIdsToPin.isEmpty()) {
        throw ConnectorRolloutNotEnoughActorsProblem(
          ProblemMessageData().message(
            "No additional actors are eligible to be pinned for the progressive rollout.",
          ),
        )
      }

      try {
        actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(
          actorSelectionInfo.actorIdsToPin.toSet(),
          connectorRollout.actorDefinitionId,
          connectorRollout.releaseCandidateVersionId,
          connectorRollout.id,
        )
      } catch (e: InvalidRequestException) {
        throw ConnectorRolloutInvalidRequestProblem(ProblemMessageData().message("Failed to create release candidate pins for actors: ${e.message}"))
      }

      connectorRollout.state = ConnectorEnumRolloutState.IN_PROGRESS
      connectorRollout.rolloutStrategy = ConnectorEnumRolloutStrategy.fromValue(rolloutStrategy.toString())
      connectorRollout.updatedAt = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond()
      return connectorRollout
    }

    @VisibleForTesting
    internal fun getValidPercentageToPin(
      connectorRollout: ConnectorRollout,
      targetPercentage: Int,
      rolloutStrategy: ConnectorRolloutStrategy,
      percentageAlreadyPinned: Int,
    ): Int {
      if (rolloutStrategy != ConnectorRolloutStrategy.AUTOMATED) {
        return targetPercentage
      }
      val maxRolloutPct =
        if (connectorRollout.finalTargetRolloutPct == null) {
          DEFAULT_MAX_ROLLOUT_PERCENTAGE
        } else {
          connectorRollout.finalTargetRolloutPct!!
        }

      val actualTargetRolloutPct = min(targetPercentage, maxRolloutPct)
      if (targetPercentage > actualTargetRolloutPct) {
        logger.info { "Requested to pin $targetPercentage% of actors but capped at $actualTargetRolloutPct." }
      }

      if (percentageAlreadyPinned >= actualTargetRolloutPct) {
        throw ConnectorRolloutMaximumRolloutPercentageReachedProblem(
          ProblemMessageData().message(
            "Requested to pin $actualTargetRolloutPct% of actors but already pinned ${connectorRollout.currentTargetRolloutPct}.",
          ),
        )
      }
      return actualTargetRolloutPct
    }

    @VisibleForTesting
    open fun getAndValidateFinalizeRequest(connectorRolloutFinalize: ConnectorRolloutFinalizeRequestBody): ConnectorRollout {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutFinalize.id)
      val invalidFinalizeStates = ConnectorRolloutFinalState.entries.map { ConnectorEnumRolloutState.fromValue(it.toString()) }
      if (connectorRollout.state in invalidFinalizeStates
      ) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Connector rollout may not be in a terminal state when finalizing the rollout, but was in state " + connectorRollout.state.toString(),
          ),
        )
      }

      // Unpin all actors that are pinned to the release candidate, unless the state is CANCELED and the user opted to retain pins on cancellation
      // so that the same actors will be pinned to the next release candidate.
      if (!(connectorRolloutFinalize.state == ConnectorRolloutStateTerminal.CANCELED && connectorRolloutFinalize.retainPinsOnCancellation)) {
        actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(
          connectorRollout.actorDefinitionId,
          connectorRollout.releaseCandidateVersionId,
        )
      }

      val currentTime = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond()

      connectorRollout.state = ConnectorEnumRolloutState.fromValue(connectorRolloutFinalize.state.toString())
      connectorRollout.rolloutStrategy = ConnectorEnumRolloutStrategy.fromValue(connectorRolloutFinalize.rolloutStrategy.toString())
      connectorRollout.updatedAt = currentTime
      connectorRollout.completedAt = currentTime
      connectorRollout.errorMsg = connectorRolloutFinalize.errorMsg
      connectorRollout.failedReason = connectorRolloutFinalize.failedReason

      return connectorRollout
    }

    @VisibleForTesting
    open fun getAndValidateUpdateStateRequest(
      id: UUID,
      state: ConnectorEnumRolloutState,
      errorMsg: String?,
      failedReason: String?,
      pausedReason: String?,
    ): ConnectorRollout {
      val connectorRollout = connectorRolloutService.getConnectorRollout(id)
      val invalidUpdateStates = ConnectorRolloutFinalState.entries.map { ConnectorEnumRolloutState.fromValue(it.toString()) }
      if (connectorRollout.state in invalidUpdateStates
      ) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Connector rollout may not be in a terminal state when updating the rollout state, but was in state " + connectorRollout.state.toString(),
          ),
        )
      }
      connectorRollout.state = state
      connectorRollout.errorMsg = errorMsg
      connectorRollout.failedReason = failedReason
      connectorRollout.pausedReason = pausedReason

      return connectorRollout
    }

    private fun unixTimestampToOffsetDateTime(unixTimestamp: Long): OffsetDateTime = Instant.ofEpochSecond(unixTimestamp).atOffset(ZoneOffset.UTC)

    @Transactional("config")
    open fun startConnectorRollout(connectorRolloutStart: ConnectorRolloutStartRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndValidateStartRequest(connectorRolloutStart)

      // If actors are still pinned to a previous rollout's release candidate, we migrate them to the new release candidate
      if (connectorRolloutStart.migratePins) {
        actorDefinitionVersionUpdater.migrateReleaseCandidatePins(
          connectorRollout.actorDefinitionId,
          connectorRolloutService.listConnectorRollouts(connectorRollout.actorDefinitionId).map { it.id.toString() },
          connectorRollout.id.toString(),
          connectorRollout.releaseCandidateVersionId,
        )
        connectorRollout.currentTargetRolloutPct = getPercentagePinned(getActorSelectionInfo(connectorRollout, 0))
      }

      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return connectorRolloutHelper.buildConnectorRolloutRead(updatedConnectorRollout, true)
    }

    @Transactional("config")
    open fun doConnectorRollout(connectorRolloutUpdate: ConnectorRolloutRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndRollOutConnectorRollout(connectorRolloutUpdate)
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return connectorRolloutHelper.buildConnectorRolloutRead(updatedConnectorRollout, true)
    }

    @Transactional("config")
    open fun finalizeConnectorRollout(connectorRolloutFinalize: ConnectorRolloutFinalizeRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndValidateFinalizeRequest(connectorRolloutFinalize)
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return connectorRolloutHelper.buildConnectorRolloutRead(updatedConnectorRollout, true)
    }

    open fun getConnectorRollout(id: UUID): ConnectorRolloutRead {
      val connectorRollout = connectorRolloutService.getConnectorRollout(id)
      return connectorRolloutHelper.buildConnectorRolloutRead(connectorRollout, true)
    }

    open fun updateState(connectorRolloutUpdateStateRequestBody: ConnectorRolloutUpdateStateRequestBody): ConnectorRolloutRead {
      val rollout: ConnectorRollout =
        if (connectorRolloutUpdateStateRequestBody.id == null) {
          getRolloutByActorDefinitionIdAndDockerImageTag(
            connectorRolloutUpdateStateRequestBody.actorDefinitionId,
            connectorRolloutUpdateStateRequestBody.dockerImageTag,
          )
        } else {
          connectorRolloutService.getConnectorRollout(connectorRolloutUpdateStateRequestBody.id)
        }
      val connectorRollout =
        getAndValidateUpdateStateRequest(
          rollout.id,
          ConnectorEnumRolloutState.fromValue(connectorRolloutUpdateStateRequestBody.state.toString()),
          connectorRolloutUpdateStateRequestBody.errorMsg,
          connectorRolloutUpdateStateRequestBody.failedReason,
          connectorRolloutUpdateStateRequestBody.pausedReason,
        )
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return connectorRolloutHelper.buildConnectorRolloutRead(updatedConnectorRollout, true)
    }

    fun getActorSyncInfo(id: UUID): Map<UUID, ConnectorRolloutActorSyncInfo> {
      val rollout = connectorRolloutService.getConnectorRollout(id)
      val actorSyncInfoMap = rolloutActorFinder.getSyncInfoForPinnedActors(rollout)
      return actorSyncInfoMap.mapValues { (id, syncInfo) ->
        ConnectorRolloutActorSyncInfo()
          .actorId(id)
          .numConnections(syncInfo.nConnections)
          .numSucceeded(syncInfo.nSucceeded)
          .numFailed(syncInfo.nFailed)
      }
    }

    @Transactional("config")
    open fun getActorSelectionInfo(
      connectorRollout: ConnectorRollout,
      targetPercent: Int,
    ): ActorSelectionInfo {
      val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(connectorRollout, targetPercent, connectorRollout.filters)
      if (targetPercent > 0 && actorSelectionInfo.actorIdsToPin.isEmpty() && actorSelectionInfo.nPreviouslyPinned == 0) {
        throw ConnectorRolloutNotEnoughActorsProblem(
          ProblemMessageData().message(
            "No actors are eligible to be pinned for a progressive rollout.",
          ),
        )
      }
      if (targetPercent > 0 && actorSelectionInfo.actorIdsToPin.isEmpty() && actorSelectionInfo.nPreviouslyPinned > 0) {
        throw ConnectorRolloutNotEnoughActorsProblem(
          ProblemMessageData().message(
            "No new actors are eligible to be pinned for a progressive rollout.",
          ),
        )
      }
      return actorSelectionInfo
    }

    @Cacheable("rollout-updated-by")
    open fun getUpdatedBy(
      rolloutStrategy: ConnectorEnumRolloutStrategy,
      updatedById: UUID,
    ): String =
      when (rolloutStrategy) {
        ConnectorEnumRolloutStrategy.MANUAL -> userPersistence.getUser(updatedById).get().email
        else -> ""
      }

    private fun isTerminalState(state: ConnectorEnumRolloutState): Boolean =
      ConnectorRolloutFinalState.entries
        .map {
          ConnectorEnumRolloutState.fromValue(it.toString())
        }.contains(state)

    private fun getRolloutByActorDefinitionIdAndDockerImageTag(
      actorDefinitionId: UUID,
      dockerImageTag: String,
    ): ConnectorRollout {
      val actorDefinitionVersion =
        actorDefinitionService
          .getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
          .orElseThrow {
            throw ConnectorRolloutInvalidRequestProblem(
              ProblemMessageData().message(
                "Actor definition version not found for actor definition id: $actorDefinitionId and docker image tag: $dockerImageTag",
              ),
            )
          }
      val rollouts =
        connectorRolloutService.listConnectorRollouts(actorDefinitionId, actorDefinitionVersion.versionId).filter {
          !isTerminalState(it.state)
        }
      if (rollouts.size != 1) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Expected 1 rollout in a non-terminal state, found ${rollouts.size}.",
          ),
        )
      }
      return rollouts.first()
    }
  }
