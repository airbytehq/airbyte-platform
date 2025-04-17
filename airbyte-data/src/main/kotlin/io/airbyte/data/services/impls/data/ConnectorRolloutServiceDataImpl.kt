/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.ConnectorRolloutRepository
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.db.instance.configs.jooq.generated.enums.ConnectorRolloutStateType
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
open class ConnectorRolloutServiceDataImpl(
  private val repository: ConnectorRolloutRepository,
) : ConnectorRolloutService {
  override fun getConnectorRollout(id: UUID): ConnectorRollout =
    repository
      .findById(id)
      .orElseThrow {
        ConfigNotFoundException("ConnectorRollout", id.toString())
      }.toConfigModel()

  override fun insertConnectorRollout(connectorRollout: ConnectorRollout): ConnectorRollout {
    // We only want to have a single initialized rollout per actor definition id and release candidate version id
    // (unless the previous rollout was canceled)
    repository
      .findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        connectorRollout.actorDefinitionId,
        connectorRollout.releaseCandidateVersionId,
      ).firstOrNull { it.state != ConnectorRolloutStateType.canceled }
      ?.let {
        throw RuntimeException(
          "A rollout in state ${it.state} already exists for actor definition id ${it.actorDefinitionId} " +
            "and version id ${it.releaseCandidateVersionId}",
        )
      }

    val existingConnectorRolloutOnActorDefinitionId = repository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(connectorRollout.actorDefinitionId)

    val connectorRolloutEnumFinalStates: List<ConnectorEnumRolloutState> =
      ConnectorRolloutFinalState.entries.map {
        ConnectorEnumRolloutState.valueOf(it.name)
      }

    for (existingConnectorRollout in existingConnectorRolloutOnActorDefinitionId) {
      if (ConnectorEnumRolloutState.valueOf(existingConnectorRollout.state.name.uppercase()) !in connectorRolloutEnumFinalStates) {
        throw RuntimeException(
          "A connector rollout is incomplete (current state: ${existingConnectorRollout.state.name}) " +
            "for this actor definition id ${connectorRollout.actorDefinitionId} " +
            "on version ${existingConnectorRollout.releaseCandidateVersionId} ",
        )
      }
    }

    return repository.save(connectorRollout.toEntity()).toConfigModel()
  }

  override fun writeConnectorRollout(connectorRollout: ConnectorRollout): ConnectorRollout {
    val entity = connectorRollout.toEntity()

    if (repository.existsById(connectorRollout.id)) {
      logger.info { "Updating existing connector rollout: connectorRollout=$connectorRollout entity=$entity" }
      return repository.update(entity).toConfigModel()
    }
    logger.info { "Creating new connector rollout: connectorRollout=$connectorRollout entity=$entity" }
    return repository.save(entity).toConfigModel()
  }

  override fun listConnectorRollouts(): List<ConnectorRollout> =
    repository.findAllOrderByUpdatedAtDesc().map { unit ->
      unit.toConfigModel()
    }

  override fun listConnectorRollouts(actorDefinitionId: UUID): List<ConnectorRollout> =
    repository
      .findAllByActorDefinitionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
      ).map { unit ->
        unit.toConfigModel()
      }

  override fun listConnectorRollouts(
    actorDefinitionId: UUID,
    releaseCandidateVersionId: UUID,
  ): List<ConnectorRollout> =
    repository
      .findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
        actorDefinitionId,
        releaseCandidateVersionId,
      ).map { unit ->
        unit.toConfigModel()
      }
}
