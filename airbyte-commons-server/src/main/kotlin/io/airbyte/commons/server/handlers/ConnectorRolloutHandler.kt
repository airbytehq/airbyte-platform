/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ConnectorRolloutCreateRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutRead
import io.airbyte.api.model.generated.ConnectorRolloutState
import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.Period
import java.time.ZoneOffset
import java.util.UUID

val ONE_DAY: Period = Period.ofDays(1)

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class ConnectorRolloutHandler
  @Inject
  constructor(
    private val connectorRolloutService: ConnectorRolloutService,
    private val actorDefinitionService: ActorDefinitionService,
  ) {
    @VisibleForTesting
    fun buildConnectorRolloutRead(connectorRollout: ConnectorRollout): ConnectorRolloutRead {
      return ConnectorRolloutRead()
        .id(connectorRollout.id)
        .actorDefinitionId(connectorRollout.actorDefinitionId)
        .releaseCandidateVersionId(connectorRollout.releaseCandidateVersionId)
        .initialVersionId(connectorRollout.initialVersionId)
        .state(ConnectorRolloutState.fromString(connectorRollout.state.toString()))
        .initialRolloutPct(connectorRollout.initialRolloutPct?.toInt())
        .currentTargetRolloutPct(connectorRollout.currentTargetRolloutPct?.toInt())
        .finalTargetRolloutPct(connectorRollout.finalTargetRolloutPct?.toInt())
        .hasBreakingChanges(connectorRollout.hasBreakingChanges)
        .rolloutStrategy(ConnectorRolloutStrategy.fromString(connectorRollout.rolloutStrategy.toString()))
        .maxStepWaitTimeMins(connectorRollout.maxStepWaitTimeMins.toInt())
        .updatedBy(connectorRollout.updatedBy)
        .createdAt(connectorRollout.createdAt?.let { unixTimestampToOffsetDateTime(it) })
        .updatedAt(connectorRollout.updatedAt?.let { unixTimestampToOffsetDateTime(it) })
        .completedAt(connectorRollout.completedAt?.let { unixTimestampToOffsetDateTime(it) })
        .expiresAt(connectorRollout.expiresAt?.let { unixTimestampToOffsetDateTime(it) })
        .errorMsg(connectorRollout.errorMsg)
        .failedReason(connectorRollout.failedReason)
    }

    @VisibleForTesting
    fun buildConnectorRollout(connectorRolloutCreate: ConnectorRolloutCreateRequestBody): ConnectorRollout {
      val currentTime = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond()
      val defaultVersion = actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(connectorRolloutCreate.actorDefinitionId)
      return ConnectorRollout()
        .withId(connectorRolloutCreate.id)
        .withActorDefinitionId(connectorRolloutCreate.actorDefinitionId)
        .withReleaseCandidateVersionId(connectorRolloutCreate.releaseCandidateVersionId)
        .withInitialVersionId(defaultVersion.orElse(null)?.versionId)
        .withState(ConnectorEnumRolloutState.INITIALIZED)
        .withInitialRolloutPct(connectorRolloutCreate.initialRolloutPct?.toLong())
        .withCurrentTargetRolloutPct(connectorRolloutCreate.currentTargetRolloutPct?.toLong())
        .withFinalTargetRolloutPct(connectorRolloutCreate.finalTargetRolloutPct?.toLong())
        .withHasBreakingChanges(connectorRolloutCreate.hasBreakingChanges)
        .withRolloutStrategy(ConnectorEnumRolloutStrategy.fromValue(connectorRolloutCreate.rolloutStrategy.toString()))
        .withMaxStepWaitTimeMins(connectorRolloutCreate.maxStepWaitTimeMins.toLong())
        .withUpdatedBy(connectorRolloutCreate.updatedBy)
        .withCreatedAt(currentTime)
        .withUpdatedAt(currentTime)
        .withCompletedAt(null)
        .withExpiresAt(currentTime.plus(ONE_DAY.days))
        .withErrorMsg(connectorRolloutCreate.errorMsg)
        .withFailedReason(connectorRolloutCreate.failedReason)
    }

    private fun unixTimestampToOffsetDateTime(unixTimestamp: Long): OffsetDateTime {
      return Instant.ofEpochSecond(unixTimestamp).atOffset(ZoneOffset.UTC)
    }

    @Transactional
    open fun listConnectorRollouts(
      actorDefinitionId: UUID,
      dockerImageTag: String,
    ): List<ConnectorRolloutRead> {
      val actorDefinition = actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag)
      if (actorDefinition.isEmpty) {
        return emptyList()
      }
      val connectorRollouts: List<ConnectorRollout> =
        connectorRolloutService.listConnectorRollouts(
          actorDefinitionId,
          actorDefinition.get().versionId,
        )
      return connectorRollouts.map { connectorRollout ->
        buildConnectorRolloutRead(connectorRollout)
      }
    }

    fun insertConnectorRollout(connectorRolloutCreate: ConnectorRolloutCreateRequestBody): ConnectorRolloutRead {
      val connectorRollout = buildConnectorRollout(connectorRolloutCreate)
      val insertedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)

      return buildConnectorRolloutRead(insertedConnectorRollout)
    }

    fun getConnectorRollout(id: UUID): ConnectorRolloutRead {
      val connectorRollout = connectorRolloutService.getConnectorRollout(id)
      return buildConnectorRolloutRead(connectorRollout)
    }

    fun updateConnectorRollout(
      id: UUID,
      connectorRolloutCreate: ConnectorRolloutCreateRequestBody,
    ): ConnectorRolloutRead {
      val connectorRollout = buildConnectorRollout(connectorRolloutCreate).withId(id)
      val updatedConnectorRollout = connectorRolloutService.writeConnectorRollout(connectorRollout)
      return buildConnectorRolloutRead(updatedConnectorRollout)
    }
  }
