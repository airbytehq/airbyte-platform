package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigSchema
import io.airbyte.config.ConnectorRollout
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.ConnectorRolloutRepository
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class ConnectorRolloutServiceDataImpl(private val repository: ConnectorRolloutRepository) : ConnectorRolloutService {
  override fun getConnectorRollout(id: UUID): ConnectorRollout {
    return repository.findById(id).orElseThrow {
      ConfigNotFoundException(ConfigSchema.CONNECTOR_ROLLOUT, id)
    }.toConfigModel()
  }

  override fun writeConnectorRollout(connectorRollout: ConnectorRollout): ConnectorRollout {
    if (repository.existsById(connectorRollout.id)) {
      return repository.update(connectorRollout.toEntity()).toConfigModel()
    }
    return repository.save(connectorRollout.toEntity()).toConfigModel()
  }

  override fun listConnectorRollouts(): List<ConnectorRollout> {
    return repository.findAllOrderByUpdatedAtDesc().map { unit ->
      unit.toConfigModel()
    }
  }

  override fun listConnectorRollouts(actorDefinitionId: UUID): List<ConnectorRollout> {
    return repository.findAllByActorDefinitionIdOrderByUpdatedAtDesc(
      actorDefinitionId,
    ).map { unit ->
      unit.toConfigModel()
    }
  }

  override fun listConnectorRollouts(
    actorDefinitionId: UUID,
    releaseCandidateVersionId: UUID,
  ): List<ConnectorRollout> {
    return repository.findAllByActorDefinitionIdAndReleaseCandidateVersionIdOrderByUpdatedAtDesc(
      actorDefinitionId,
      releaseCandidateVersionId,
    ).map { unit ->
      unit.toConfigModel()
    }
  }
}
