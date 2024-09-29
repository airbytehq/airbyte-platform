/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ConnectorRolloutCreateRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutState
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.commons.server.validation.InvalidRequest
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.connector.rollout.client.ConnectorRolloutClient
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
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
    private val connectorRolloutClient: ConnectorRolloutClient,
    private val userPersistence: UserPersistence,
  ) {
    @VisibleForTesting
    open fun buildConnectorRolloutRead(connectorRollout: ConnectorRollout): ConnectorRolloutRead {
      val rolloutStrategy = connectorRollout.rolloutStrategy?.let { ConnectorRolloutStrategy.fromValue(it.toString()) }

      val actorDefinitionVersion = actorDefinitionService.getActorDefinitionVersion(connectorRollout.releaseCandidateVersionId)
      return ConnectorRolloutRead()
        .id(connectorRollout.id)
        .dockerRepository(actorDefinitionVersion.dockerRepository)
        .dockerImageTag(actorDefinitionVersion.dockerImageTag)
        .workflowRunId(connectorRollout.workflowRunId)
        .actorDefinitionId(connectorRollout.actorDefinitionId)
        .releaseCandidateVersionId(connectorRollout.releaseCandidateVersionId)
        .initialVersionId(connectorRollout.initialVersionId)
        .state(ConnectorRolloutState.fromString(connectorRollout.state.toString()))
        .initialRolloutPct(connectorRollout.initialRolloutPct?.toInt())
        .currentTargetRolloutPct(connectorRollout.currentTargetRolloutPct?.toInt())
        .finalTargetRolloutPct(connectorRollout.finalTargetRolloutPct?.toInt())
        .hasBreakingChanges(connectorRollout.hasBreakingChanges)
        .rolloutStrategy(rolloutStrategy)
        .maxStepWaitTimeMins(connectorRollout.maxStepWaitTimeMins?.toInt())
        .updatedAt(connectorRollout.updatedAt?.let { unixTimestampToOffsetDateTime(it) })
        .createdAt(connectorRollout.createdAt?.let { unixTimestampToOffsetDateTime(it) })
        .expiresAt(connectorRollout.expiresAt?.let { unixTimestampToOffsetDateTime(it) })
        .errorMsg(connectorRollout.errorMsg)
        .failedReason(connectorRollout.failedReason)
        .updatedBy(
          connectorRollout.rolloutStrategy?.let { strategy ->
            connectorRollout.updatedBy?.let { updatedBy ->
              getUpdatedBy(strategy, updatedBy)
            }
          },
        )
        .completedAt(connectorRollout.completedAt?.let { unixTimestampToOffsetDateTime(it) })
        .expiresAt(connectorRollout.expiresAt?.let { unixTimestampToOffsetDateTime(it) })
        .errorMsg(connectorRollout.errorMsg)
        .failedReason(connectorRollout.failedReason)
    }

    @VisibleForTesting
    open fun buildConnectorRollout(connectorRolloutCreate: ConnectorRolloutCreateRequestBody): ConnectorRollout {
      val defaultVersion = actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(connectorRolloutCreate.actorDefinitionId)
      val rolloutVersion =
        actorDefinitionService.getActorDefinitionVersion(
          connectorRolloutCreate.actorDefinitionId,
          connectorRolloutCreate.dockerImageTag,
        )
      if (rolloutVersion.isEmpty) {
        throw InvalidRequest(
          "Could not find actor definition version for actor definition id: " +
            "${connectorRolloutCreate.actorDefinitionId} and docker image tag: ${connectorRolloutCreate.dockerImageTag}",
        )
      }

      return ConnectorRollout()
        .withId(UUID.randomUUID())
        .withWorkflowRunId(null) // This will be populated once the workflow has started
        .withActorDefinitionId(connectorRolloutCreate.actorDefinitionId)
        .withReleaseCandidateVersionId(rolloutVersion.get().versionId)
        .withInitialVersionId(defaultVersion.orElse(null)?.versionId)
        .withState(ConnectorEnumRolloutState.INITIALIZED)
        .withHasBreakingChanges(connectorRolloutCreate.hasBreakingChanges)
    }

    @VisibleForTesting
    open fun getAndValidateInsertRequest(connectorRolloutCreate: ConnectorRolloutCreateRequestBody): ConnectorRollout {
      validateRolloutActorDefinitionId(
        connectorRolloutCreate.dockerRepository,
        connectorRolloutCreate.dockerImageTag,
        connectorRolloutCreate.actorDefinitionId,
      )
      val existingRollouts = listConnectorRollouts(connectorRolloutCreate.actorDefinitionId, connectorRolloutCreate.dockerImageTag)
      if (existingRollouts.isNotEmpty()) {
        existingRollouts.forEach { rollout ->
          // We should only be creating a new rollout for the same actor definition + release candidate version if the previous rollout errored
          // and was therefore canceled.
          // If the rollout succeeded we shouldn't be re-rolling out, and if it failed then there should be a code change that will bump the version.
          if (rollout.state != ConnectorRolloutState.CANCELED_ROLLED_BACK) {
            throw InvalidRequest("Cannot insert new rollout: Active or non-canceled rollout(s) exist.")
          }
        }
      }
      return buildConnectorRollout(connectorRolloutCreate)
    }

    @VisibleForTesting
    open fun validateRolloutActorDefinitionId(
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
    ) {
      val actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
          .orElseThrow {
            InvalidRequest("Actor definition version not found for actor definition id: $actorDefinitionId and docker image tag: $dockerImageTag")
          }

      if (actorDefinitionVersion.dockerRepository != dockerRepository || actorDefinitionVersion.dockerImageTag != dockerImageTag) {
        throw InvalidRequest("Actor definition ID does not match docker repository: $dockerRepository and docker image tag: $dockerImageTag")
      }
    }

    open fun getOrCreateAndValidateManualStartInput(
      dockerRepository: String,
      actorDefinitionId: UUID,
      dockerImageTag: String,
      updatedBy: UUID,
    ): ConnectorRollout {
      val actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(
          actorDefinitionId,
          dockerImageTag,
        )
      if (actorDefinitionVersion.isEmpty) {
        throw InvalidRequest(
          "Actor definition version not found for actor definition id: $actorDefinitionId " +
            "and docker image tag: $dockerImageTag",
        )
      }
      if (actorDefinitionVersion.get().dockerRepository != dockerRepository) {
        throw InvalidRequest(
          "Actor definition version does not match docker repository: $dockerRepository ",
        )
      }
      val connectorRollouts: List<ConnectorRollout> =
        connectorRolloutService.listConnectorRollouts(
          actorDefinitionId,
          actorDefinitionVersion.get().versionId,
        )

      val initializedRollouts = connectorRollouts.filter { it.state == ConnectorEnumRolloutState.INITIALIZED }
      val initialVersion =
        actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId)
          ?: throw InvalidRequest("Could not find initial version for actor definition id: $actorDefinitionId")

      if (initializedRollouts.isEmpty()) {
        val connectorRollout =
          ConnectorRollout()
            .withId(UUID.randomUUID())
            .withActorDefinitionId(actorDefinitionId)
            .withReleaseCandidateVersionId(actorDefinitionVersion.get().versionId)
            .withInitialVersionId(initialVersion.get().versionId)
            .withUpdatedBy(updatedBy)
            .withState(ConnectorEnumRolloutState.INITIALIZED)
            .withHasBreakingChanges(false)
        connectorRolloutService.writeConnectorRollout(connectorRollout)
        return connectorRollout
      }

      if (initializedRollouts.size > 1) {
        throw InvalidRequest("Expected at most 1 rollout in the INITIALIZED state, found ${initializedRollouts.size}.")
      }
      val finalEnumStates =
        ConnectorEnumRolloutState.entries.filter {
          ConnectorRolloutFinalState.entries.map { it.toString() }.contains(
            it.toString(),
          )
        }
      val rolloutsInInvalidState =
        connectorRollouts.filter { rollout: ConnectorRollout ->
          finalEnumStates.contains(rollout.state) &&
            (rollout.state != ConnectorEnumRolloutState.INITIALIZED)
        }

      if (rolloutsInInvalidState.isNotEmpty()) {
        throw InvalidRequest("Found rollouts in invalid states: ${rolloutsInInvalidState.map { it.id }}.")
      }
      return initializedRollouts.first()
    }

    @VisibleForTesting
    open fun getAndValidateStartRequest(connectorRolloutStart: ConnectorRolloutStartRequestBody): ConnectorRollout {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutStart.id)
      if (connectorRollout.state != ConnectorEnumRolloutState.INITIALIZED) {
        throw InvalidRequest(
          "Connector rollout must be in INITIALIZED state to start the rollout, but was in state " + connectorRollout.state.toString(),
        )
      }
      return connectorRollout
        .withWorkflowRunId(connectorRolloutStart.workflowRunId)
        .withState(ConnectorEnumRolloutState.WORKFLOW_STARTED)
        .withRolloutStrategy(ConnectorEnumRolloutStrategy.fromValue(connectorRolloutStart.rolloutStrategy.toString()))
    }

    @VisibleForTesting
    open fun getAndRollOutConnectorRollout(connectorRolloutRequest: ConnectorRolloutRequestBody): ConnectorRollout {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutRequest.id)
      if (connectorRollout.state !in
        setOf(
          ConnectorEnumRolloutState.WORKFLOW_STARTED,
          ConnectorEnumRolloutState.IN_PROGRESS,
          ConnectorEnumRolloutState.PAUSED,
        )
      ) {
        throw InvalidRequest(
          "Connector rollout must be in WORKFLOW_STARTED, IN_PROGRESS, or PAUSED state to update the rollout, but was in state " +
            connectorRollout.state.toString(),
        )
      }

      try {
        actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(
          connectorRolloutRequest.actorIds.toSet(),
          connectorRollout.actorDefinitionId,
          connectorRollout.initialVersionId,
          connectorRollout.releaseCandidateVersionId,
        )
      } catch (e: InvalidRequestException) {
        throw InvalidRequest("Failed to create release candidate pins for actors: ${e.message}")
      }

      return connectorRollout
        .withState(ConnectorEnumRolloutState.IN_PROGRESS)
        .withRolloutStrategy(ConnectorEnumRolloutStrategy.fromValue(connectorRolloutRequest.rolloutStrategy.toString()))
        .withUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond())
    }

    @VisibleForTesting
    open fun getAndValidateFinalizeRequest(connectorRolloutFinalize: ConnectorRolloutFinalizeRequestBody): ConnectorRollout {
      val connectorRollout = connectorRolloutService.getConnectorRollout(connectorRolloutFinalize.id)
      val invalidFinalizeStates = ConnectorRolloutFinalState.entries.map { ConnectorEnumRolloutState.fromValue(it.toString()) }
      if (connectorRollout.state in invalidFinalizeStates
      ) {
        throw InvalidRequest(
          "Connector rollout may not be in a terminal state when finalizing the rollout, but was in state " + connectorRollout.state.toString(),
        )
      }
      val currentTime = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond()
      actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(
        connectorRollout.actorDefinitionId,
        connectorRollout.releaseCandidateVersionId,
      )
      return connectorRollout
        .withState(ConnectorEnumRolloutState.fromValue(connectorRolloutFinalize.state.toString()))
        .withRolloutStrategy(ConnectorEnumRolloutStrategy.fromValue(connectorRolloutFinalize.rolloutStrategy.toString()))
        .withUpdatedAt(currentTime)
        .withCompletedAt(currentTime)
        .withErrorMsg(connectorRolloutFinalize.errorMsg)
        .withFailedReason(connectorRolloutFinalize.failedReason)
    }

    private fun unixTimestampToOffsetDateTime(unixTimestamp: Long): OffsetDateTime {
      return Instant.ofEpochSecond(unixTimestamp).atOffset(ZoneOffset.UTC)
    }

    open fun listConnectorRollouts(): List<ConnectorRolloutRead> {
      val connectorRollouts: List<ConnectorRollout> = connectorRolloutService.listConnectorRollouts()
      return connectorRollouts.map { connectorRollout ->
        buildConnectorRolloutRead(connectorRollout)
      }
    }

    open fun listConnectorRollouts(actorDefinitionId: UUID): List<ConnectorRolloutRead> {
      val connectorRollouts: List<ConnectorRollout> =
        connectorRolloutService.listConnectorRollouts(actorDefinitionId)
      return connectorRollouts.map { connectorRollout ->
        buildConnectorRolloutRead(connectorRollout)
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
        buildConnectorRolloutRead(connectorRollout)
      }
    }

    open fun insertConnectorRollout(connectorRolloutCreate: ConnectorRolloutCreateRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndValidateInsertRequest(connectorRolloutCreate)
      val insertedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return buildConnectorRolloutRead(insertedConnectorRollout)
    }

    @Transactional("config")
    open fun startConnectorRollout(connectorRolloutStart: ConnectorRolloutStartRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndValidateStartRequest(connectorRolloutStart)
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return buildConnectorRolloutRead(updatedConnectorRollout)
    }

    @Transactional("config")
    open fun doConnectorRollout(connectorRolloutUpdate: ConnectorRolloutRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndRollOutConnectorRollout(connectorRolloutUpdate)
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return buildConnectorRolloutRead(updatedConnectorRollout)
    }

    @Transactional("config")
    open fun finalizeConnectorRollout(connectorRolloutFinalize: ConnectorRolloutFinalizeRequestBody): ConnectorRolloutRead {
      val connectorRollout = getAndValidateFinalizeRequest(connectorRolloutFinalize)
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return buildConnectorRolloutRead(updatedConnectorRollout)
    }

    open fun getConnectorRollout(id: UUID): ConnectorRolloutRead {
      val connectorRollout = connectorRolloutService.getConnectorRollout(id)
      return buildConnectorRolloutRead(connectorRollout)
    }

    open fun manualStartConnectorRollout(connectorRolloutWorkflowStart: ConnectorRolloutManualStartRequestBody): ConnectorRolloutRead {
      val rollout =
        getOrCreateAndValidateManualStartInput(
          connectorRolloutWorkflowStart.dockerRepository,
          connectorRolloutWorkflowStart.actorDefinitionId,
          connectorRolloutWorkflowStart.dockerImageTag,
          connectorRolloutWorkflowStart.updatedBy,
        )
      connectorRolloutClient.startWorkflow(
        ConnectorRolloutActivityInputStart(
          connectorRolloutWorkflowStart.dockerRepository,
          connectorRolloutWorkflowStart.dockerImageTag,
          connectorRolloutWorkflowStart.actorDefinitionId,
          rollout.id,
        ),
      )

      return buildConnectorRolloutRead(connectorRolloutService.getConnectorRollout(rollout.id))
    }

    open fun manualDoConnectorRolloutWorkflowUpdate(connectorRolloutWorkflowUpdate: ConnectorRolloutManualRolloutRequestBody): ConnectorRolloutRead {
      connectorRolloutClient.doRollout(
        ConnectorRolloutActivityInputRollout(
          connectorRolloutWorkflowUpdate.dockerRepository,
          connectorRolloutWorkflowUpdate.dockerImageTag,
          connectorRolloutWorkflowUpdate.actorDefinitionId,
          connectorRolloutWorkflowUpdate.id,
          connectorRolloutWorkflowUpdate.actorIds,
        ),
      )
      return buildConnectorRolloutRead(connectorRolloutService.getConnectorRollout(connectorRolloutWorkflowUpdate.id))
    }

    open fun manualFinalizeConnectorRolloutWorkflowUpdate(
      connectorRolloutFinalizeWorkflowUpdate: ConnectorRolloutManualFinalizeRequestBody,
    ): ConnectorRolloutRead {
      connectorRolloutClient.finalizeRollout(
        ConnectorRolloutActivityInputFinalize(
          connectorRolloutFinalizeWorkflowUpdate.dockerRepository,
          connectorRolloutFinalizeWorkflowUpdate.dockerImageTag,
          connectorRolloutFinalizeWorkflowUpdate.actorDefinitionId,
          connectorRolloutFinalizeWorkflowUpdate.id,
          ConnectorRolloutFinalState.fromValue(connectorRolloutFinalizeWorkflowUpdate.state.toString()),
        ),
      )
      return buildConnectorRolloutRead(connectorRolloutService.getConnectorRollout(connectorRolloutFinalizeWorkflowUpdate.id))
    }

    @Cacheable("rollout-updated-by")
    open fun getUpdatedBy(
      rolloutStrategy: ConnectorEnumRolloutStrategy,
      updatedById: UUID,
    ): String {
      return when (rolloutStrategy) {
        ConnectorEnumRolloutStrategy.MANUAL -> userPersistence.getUser(updatedById).get().email
        else -> ""
      }
    }
  }
