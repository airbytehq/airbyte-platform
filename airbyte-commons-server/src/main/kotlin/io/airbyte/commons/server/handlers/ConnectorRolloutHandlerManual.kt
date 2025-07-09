/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutFilters
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeResponse
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutResponse
import io.airbyte.api.model.generated.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.api.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutInvalidRequestProblem
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.config.AttributeName
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.config.CustomerTier
import io.airbyte.config.CustomerTierFilter
import io.airbyte.config.JobBypassFilter
import io.airbyte.config.Operator
import io.airbyte.connector.rollout.client.ConnectorRolloutClient
import io.airbyte.connector.rollout.shared.Constants.AIRBYTE_API_CLIENT_EXCEPTION
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.micronaut.context.annotation.Value
import io.temporal.client.WorkflowUpdateException
import io.temporal.failure.ApplicationFailure
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class ConnectorRolloutHandlerManual
  @Inject
  constructor(
    @Value("\${airbyte.connector-rollout.timeouts.wait_between_rollout_seconds}")
    private val waitBetweenRolloutSeconds: Int,
    @Value("\${airbyte.connector-rollout.timeouts.wait_between_sync_results_queries_seconds}")
    private val waitBetweenSyncResultsQueriesSeconds: Int,
    @Value("\${airbyte.connector-rollout.timeouts.rollout_expiration_seconds}")
    private val rolloutExpirationSeconds: Int,
    private val connectorRolloutService: ConnectorRolloutService,
    private val actorDefinitionService: ActorDefinitionService,
    private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
    private val connectorRolloutClient: ConnectorRolloutClient,
    private val connectorRolloutHelper: ConnectorRolloutHelper,
  ) {
    open fun getOrCreateAndValidateManualStartInput(
      dockerRepository: String,
      actorDefinitionId: UUID,
      dockerImageTag: String,
      updatedBy: UUID?,
      rolloutStrategy: ConnectorRolloutStrategy,
      initialRolloutPct: Int?,
      finalTargetRolloutPct: Int?,
      requestFilters: ConnectorRolloutFilters?,
    ): ConnectorRollout {
      val actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(
          actorDefinitionId,
          dockerImageTag,
        )
      if (actorDefinitionVersion.isEmpty) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Actor definition version not found for actor definition id: $actorDefinitionId " +
              "and docker image tag: $dockerImageTag",
          ),
        )
      }
      if (actorDefinitionVersion.get().dockerRepository != dockerRepository) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message(
            "Actor definition version does not match docker repository: $dockerRepository ",
          ),
        )
      }
      val connectorRollouts: List<ConnectorRollout> =
        connectorRolloutService.listConnectorRollouts(
          actorDefinitionId,
          actorDefinitionVersion.get().versionId,
        )

      val initializedRollouts = connectorRollouts.filter { it.state == ConnectorEnumRolloutState.INITIALIZED }
      val initialVersion =
        actorDefinitionService
          .getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId)
          .orElseThrow {
            ConnectorRolloutInvalidRequestProblem(
              ProblemMessageData()
                .message("Could not find initial version for actor definition id: $actorDefinitionId"),
            )
          }

      if (rolloutStrategy == ConnectorRolloutStrategy.AUTOMATED) {
        validateAutomatedInitialRolloutPercent(initialRolloutPct)
      }

      val filters = createFiltersFromRequest(requestFilters)
      val tag = createTagFromFilters(filters)

      if (initializedRollouts.isEmpty()) {
        val currentTime = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond()

        val connectorRollout =
          ConnectorRollout(
            id = UUID.randomUUID(),
            actorDefinitionId = actorDefinitionId,
            releaseCandidateVersionId = actorDefinitionVersion.get().versionId,
            initialVersionId = initialVersion.versionId,
            createdAt = currentTime,
            updatedAt = currentTime,
            updatedBy = updatedBy,
            state = ConnectorEnumRolloutState.INITIALIZED,
            hasBreakingChanges = false,
            rolloutStrategy = getRolloutStrategyForManualStart(rolloutStrategy),
            initialRolloutPct = initialRolloutPct,
            finalTargetRolloutPct = finalTargetRolloutPct,
            filters = filters,
            tag = createTagFromFilters(filters),
          )
        connectorRolloutService.writeConnectorRollout(connectorRollout)
        return connectorRollout
      }

      if (initializedRollouts.size > 1 && initializedRollouts.any { it.tag == tag }) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message("Expected at most 1 rollout in the INITIALIZED state for tag $tag, found ${initializedRollouts.size}."),
        )
      }
      val finalEnumStates =
        ConnectorEnumRolloutState.entries.filter { rollout ->
          ConnectorRolloutFinalState.entries.map { it.toString() }.contains(
            rollout.toString(),
          )
        }
      val rolloutsInInvalidState =
        connectorRollouts.filter { rollout: ConnectorRollout ->
          rollout.tag == tag &&
            finalEnumStates.contains(rollout.state) &&
            (rollout.state != ConnectorEnumRolloutState.INITIALIZED && rollout.state != ConnectorEnumRolloutState.CANCELED)
        }

      if (rolloutsInInvalidState.isNotEmpty()) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message("Cannot create a new rollout; rollouts with tag $tag already exist in states: $rolloutsInInvalidState."),
        )
      }
      val connectorRollout =
        initializedRollouts
          .first()
      connectorRollout.updatedBy = updatedBy
      connectorRollout.rolloutStrategy = getRolloutStrategyForManualStart(rolloutStrategy)
      connectorRollout.initialRolloutPct = initialRolloutPct
      connectorRollout.finalTargetRolloutPct = finalTargetRolloutPct
      connectorRollout.filters = filters
      connectorRollout.tag = tag

      connectorRolloutService.writeConnectorRollout(connectorRollout)
      return connectorRollout
    }

    fun createTagFromFilters(filters: io.airbyte.config.ConnectorRolloutFilters?): String? {
      if (filters?.customerTierFilters.isNullOrEmpty()) {
        return null
      }

      return filters!!
        .customerTierFilters
        .flatMap { it.value }
        .sortedBy { it.name }
        .joinToString("-") { it.name }
    }

    open fun listConnectorRollouts(): List<ConnectorRolloutRead> {
      val connectorRollouts: List<ConnectorRollout> = connectorRolloutService.listConnectorRollouts()
      return connectorRollouts.map { connectorRollout ->
        connectorRolloutHelper.buildConnectorRolloutRead(connectorRollout, false)
      }
    }

    open fun listConnectorRollouts(actorDefinitionId: UUID): List<ConnectorRolloutRead> {
      val connectorRollouts: List<ConnectorRollout> =
        connectorRolloutService.listConnectorRollouts(actorDefinitionId)
      return connectorRollouts.map { connectorRollout ->
        connectorRolloutHelper.buildConnectorRolloutRead(connectorRollout, false)
      }
    }

    open fun listConnectorRollouts(
      actorDefinitionId: UUID,
      dockerImageTag: String,
    ): List<ConnectorRolloutRead> {
      val actorDefinitionVersion = actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
      if (actorDefinitionVersion.isEmpty) {
        return emptyList()
      }
      val connectorRollouts: List<ConnectorRollout> =
        connectorRolloutService.listConnectorRollouts(
          actorDefinitionId,
          actorDefinitionVersion.get().versionId,
        )
      return connectorRollouts.map { connectorRollout ->
        connectorRolloutHelper.buildConnectorRolloutRead(connectorRollout, false)
      }
    }

    fun validateAutomatedInitialRolloutPercent(initialRolloutPct: Int?) {
      if (initialRolloutPct == null || !(0 < initialRolloutPct && initialRolloutPct <= 100)) {
        throw ConnectorRolloutInvalidRequestProblem(
          ProblemMessageData().message("Invalid initial rollout pct: $initialRolloutPct for automated rollout"),
        )
      }
    }

    open fun manualStartConnectorRollout(connectorRolloutManualStart: ConnectorRolloutManualStartRequestBody): ConnectorRolloutRead {
      val rollout =
        getOrCreateAndValidateManualStartInput(
          connectorRolloutManualStart.dockerRepository,
          connectorRolloutManualStart.actorDefinitionId,
          connectorRolloutManualStart.dockerImageTag,
          connectorRolloutManualStart.updatedBy,
          connectorRolloutManualStart.rolloutStrategy,
          connectorRolloutManualStart.initialRolloutPct,
          connectorRolloutManualStart.finalTargetRolloutPct,
          connectorRolloutManualStart.filters,
        )

      try {
        connectorRolloutClient.startRolloutWorkflow(
          rollout,
          connectorRolloutManualStart.dockerRepository,
          connectorRolloutManualStart.dockerImageTag,
          connectorRolloutManualStart.actorDefinitionId,
          actorDefinitionService.getActorDefinitionVersion(rollout.initialVersionId).dockerImageTag,
          connectorRolloutManualStart.migratePins,
          waitBetweenRolloutSeconds,
          waitBetweenSyncResultsQueriesSeconds,
          rolloutExpirationSeconds,
          rollout.updatedBy,
          getRolloutStrategyForManualStart(connectorRolloutManualStart.rolloutStrategy),
        )
      } catch (e: WorkflowUpdateException) {
        rollout.state = ConnectorEnumRolloutState.CANCELED
        connectorRolloutService.writeConnectorRollout(rollout)
        throw throwAirbyteApiClientExceptionIfExists("startWorkflow", e)
      }

      return connectorRolloutHelper.buildConnectorRolloutRead(connectorRolloutService.getConnectorRollout(rollout.id), false)
    }

    open fun manualDoConnectorRollout(connectorRolloutUpdate: ConnectorRolloutManualRolloutRequestBody): ConnectorRolloutManualRolloutResponse {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutUpdate.id)

      try {
        connectorRolloutClient.doRollout(
          connectorRollout,
          connectorRolloutUpdate.dockerRepository,
          connectorRolloutUpdate.dockerImageTag,
          connectorRolloutUpdate.actorDefinitionId,
          connectorRolloutUpdate.actorIds,
          connectorRolloutUpdate.targetPercentage,
          connectorRolloutUpdate.updatedBy,
          getRolloutStrategyForManualUpdate(connectorRollout.rolloutStrategy),
        )
      } catch (e: WorkflowUpdateException) {
        throw throwAirbyteApiClientExceptionIfExists("doRollout", e)
      }
      val response = ConnectorRolloutManualRolloutResponse()
      response.status("ok")
      return response
    }

    open fun manualFinalizeConnectorRollout(
      connectorRolloutFinalize: ConnectorRolloutManualFinalizeRequestBody,
    ): ConnectorRolloutManualFinalizeResponse {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutFinalize.id)

      logger.info {
        "Finalizing rollout for ${connectorRolloutFinalize.id}; " +
          "dockerRepository=${connectorRolloutFinalize.dockerRepository}" +
          "dockerImageTag=${connectorRolloutFinalize.dockerImageTag}" +
          "actorDefinitionId=${connectorRolloutFinalize.actorDefinitionId}"
      }
      if (ConnectorRolloutFinalState.fromValue(connectorRolloutFinalize.state.toString()) == ConnectorRolloutFinalState.CANCELED) {
        cancelRollout(connectorRollout, connectorRolloutFinalize)
      } else {
        try {
          connectorRolloutClient.finalizeRollout(
            connectorRollout,
            connectorRolloutFinalize.dockerRepository,
            connectorRolloutFinalize.dockerImageTag,
            connectorRolloutFinalize.actorDefinitionId,
            actorDefinitionService.getActorDefinitionVersion(connectorRollout.initialVersionId).dockerImageTag,
            ConnectorRolloutFinalState.fromValue(connectorRolloutFinalize.state.toString()),
            connectorRolloutFinalize.errorMsg,
            connectorRolloutFinalize.failedReason,
            connectorRolloutFinalize.updatedBy,
            getRolloutStrategyForManualUpdate(connectorRollout.rolloutStrategy),
            connectorRolloutFinalize.retainPinsOnCancellation,
          )
        } catch (e: WorkflowUpdateException) {
          throw throwAirbyteApiClientExceptionIfExists("finalizeRollout", e)
        }
      }

      val response = ConnectorRolloutManualFinalizeResponse()
      response.status("ok")
      return response
    }

    fun cancelRollout(
      connectorRollout: ConnectorRollout,
      connectorRolloutFinalize: ConnectorRolloutManualFinalizeRequestBody,
    ) {
      connectorRollout.state = ConnectorEnumRolloutState.CANCELED
      connectorRollout.updatedBy = connectorRolloutFinalize.updatedBy
      connectorRollout.updatedAt = Instant.now().toEpochMilli()
      connectorRollout.rolloutStrategy = getRolloutStrategyForManualUpdate(connectorRollout.rolloutStrategy)
      connectorRollout.errorMsg = connectorRolloutFinalize.errorMsg
      connectorRollout.failedReason = connectorRolloutFinalize.failedReason
      connectorRolloutService.writeConnectorRollout(connectorRollout)

      if (connectorRolloutFinalize.retainPinsOnCancellation == false) {
        actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(
          connectorRollout.actorDefinitionId,
          connectorRollout.releaseCandidateVersionId,
        )
      }
      try {
        connectorRolloutClient.cancelRollout(
          connectorRollout,
          connectorRolloutFinalize.dockerRepository,
          connectorRolloutFinalize.dockerImageTag,
          connectorRolloutFinalize.actorDefinitionId,
          connectorRolloutFinalize.errorMsg,
          connectorRolloutFinalize.failedReason,
        )
      } catch (e: WorkflowUpdateException) {
        throw throwAirbyteApiClientExceptionIfExists("finalizeRollout", e)
      }
    }

    open fun manualPauseConnectorRollout(connectorRolloutPause: ConnectorRolloutUpdateStateRequestBody): ConnectorRolloutRead {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutPause.id)

      logger.info {
        "Pausing rollout for ${connectorRolloutPause.id}; " +
          "dockerRepository=${connectorRolloutPause.dockerRepository}" +
          "dockerImageTag=${connectorRolloutPause.dockerImageTag}" +
          "actorDefinitionId=${connectorRolloutPause.actorDefinitionId}"
      }
      try {
        connectorRolloutClient.pauseRollout(
          connectorRollout,
          connectorRolloutPause.dockerRepository,
          connectorRolloutPause.dockerImageTag,
          connectorRolloutPause.actorDefinitionId,
          connectorRolloutPause.pausedReason,
          connectorRolloutPause.updatedBy,
          getRolloutStrategyForManualUpdate(connectorRollout.rolloutStrategy),
        )
      } catch (e: WorkflowUpdateException) {
        throw throwAirbyteApiClientExceptionIfExists("pauseRollout", e)
      }
      return connectorRolloutHelper.buildConnectorRolloutRead(connectorRollout, false)
    }

    open fun getActorSelectionInfoForPinnedActors(connectorRolloutId: UUID): ConnectorRolloutActorSelectionInfo =
      connectorRolloutHelper.getActorSelectionInfoForPinnedActors(connectorRolloutId)

    internal fun getRolloutStrategyForManualUpdate(currentRolloutStrategy: ConnectorEnumRolloutStrategy?): ConnectorEnumRolloutStrategy =
      if (currentRolloutStrategy == null || currentRolloutStrategy == ConnectorEnumRolloutStrategy.MANUAL) {
        ConnectorEnumRolloutStrategy.MANUAL
      } else {
        ConnectorEnumRolloutStrategy.OVERRIDDEN
      }

    internal fun getRolloutStrategyForManualStart(rolloutStrategy: ConnectorRolloutStrategy?): ConnectorEnumRolloutStrategy =
      if (rolloutStrategy == null || rolloutStrategy == ConnectorRolloutStrategy.MANUAL) {
        ConnectorEnumRolloutStrategy.MANUAL
      } else {
        ConnectorEnumRolloutStrategy.AUTOMATED
      }

    private fun createFiltersFromRequest(requestFilters: ConnectorRolloutFilters?): io.airbyte.config.ConnectorRolloutFilters =
      io.airbyte.config.ConnectorRolloutFilters(
        jobBypassFilter =
          if (requestFilters?.jobBypassFilter?.shouldIgnoreJobs != null) {
            JobBypassFilter(AttributeName.BYPASS_JOBS, requestFilters.jobBypassFilter?.shouldIgnoreJobs!!)
          } else {
            null
          },
        customerTierFilters =
          if (requestFilters?.tierFilter == null) {
            listOf(
              CustomerTierFilter(
                name = AttributeName.TIER,
                operator = Operator.IN,
                value = listOf(CustomerTier.TIER_2),
              ),
            )
          } else if (requestFilters.tierFilter?.tier == io.airbyte.api.model.generated.CustomerTier.ALL) {
            emptyList()
          } else {
            listOf(
              CustomerTierFilter(
                name = AttributeName.TIER,
                operator = Operator.IN,
                value = listOf(CustomerTier.valueOf(requestFilters.tierFilter!!.tier.toString())),
              ),
            )
          },
      )

    private fun throwAirbyteApiClientExceptionIfExists(
      handlerName: String,
      e: WorkflowUpdateException,
    ): Throwable {
      if (isAirbyteApiClientException(e)) {
        throw ConnectorRolloutInvalidRequestProblem(
          extractAirbyteApiClientException(e),
          ProblemMessageData().message("An error occurred in the `$handlerName` update handler."),
        )
      } else {
        throw e
      }
    }

    @VisibleForTesting
    fun extractAirbyteApiClientException(e: WorkflowUpdateException): String {
      val exc = getAirbyteApiClientException(e) as ApplicationFailure
      logger.error { "AirbyteApiClientException: $exc" }
      return exc.originalMessage
    }

    private fun isAirbyteApiClientException(e: WorkflowUpdateException): Boolean {
      val cause = e.cause?.cause
      if (cause is ApplicationFailure) {
        return cause.type == AIRBYTE_API_CLIENT_EXCEPTION
      }
      return false
    }

    private fun getAirbyteApiClientException(e: WorkflowUpdateException): Throwable = e.cause?.cause ?: e.cause ?: e
  }
