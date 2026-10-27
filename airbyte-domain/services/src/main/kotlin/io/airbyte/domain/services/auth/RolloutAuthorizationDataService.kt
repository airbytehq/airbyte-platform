/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import jakarta.inject.Singleton
import java.util.UUID

data class RolloutAuthorizationMetadata(
  val id: UUID,
  val definition: UUID,
  val candidate: UUID,
  val initial: UUID,
  val repository: String,
  val tag: String,
)

data class RolloutCandidate(
  val definition: UUID,
  val repository: String,
  val tag: String,
)

class RolloutReferenceMismatchException(
  message: String,
) : RuntimeException(message)

@Singleton
class RolloutAuthorizationDataService(
  private val data: AuthorizationDataService,
  private val rollouts: ConnectorRolloutService,
  private val definitions: ActorDefinitionService,
) {
  fun saved(id: UUID): RolloutAuthorizationMetadata {
    val rollout =
      try {
        rollouts.getConnectorRollout(id)
      } catch (_: ConfigNotFoundException) {
        throw AuthorizationResourceNotFoundException()
      }
    if (rollout.id != id) throw AuthorizationResourceNotFoundException()
    data.requireSavedActorDefinition(rollout.actorDefinitionId)
    val candidate = version(rollout.releaseCandidateVersionId, rollout.actorDefinitionId)
    version(rollout.initialVersionId, rollout.actorDefinitionId)
    return RolloutAuthorizationMetadata(
      id,
      rollout.actorDefinitionId,
      rollout.releaseCandidateVersionId,
      rollout.initialVersionId,
      candidate.dockerRepository,
      candidate.dockerImageTag,
    )
  }

  fun candidate(request: RolloutCandidate) {
    data.requireSavedActorDefinition(request.definition)
    val candidate = taggedVersion(request.definition, request.tag)
    if (candidate.dockerRepository !=
      request.repository
    ) {
      throw RolloutReferenceMismatchException("Actor definition version does not match docker repository: ${request.repository} ")
    }
    val initial =
      definitions.getDefaultVersionForActorDefinitionIdOptional(request.definition).orElseThrow {
        RolloutReferenceMismatchException("Could not find initial version for actor definition id: ${request.definition}")
      }
    if (initial.actorDefinitionId != request.definition || initial.versionId == null) throw AuthorizationResourceNotFoundException()
  }

  fun byDefinitionAndTag(
    definition: UUID,
    tag: String,
  ): RolloutAuthorizationMetadata {
    val candidate = taggedVersion(definition, tag)
    val terminal = ConnectorRolloutFinalState.entries.map { it.toString() }.toSet()
    val matching = rollouts.listConnectorRollouts(definition, candidate.versionId).filter { it.state.toString() !in terminal }
    if (matching.size != 1) throw RolloutReferenceMismatchException("Expected 1 rollout in a non-terminal state, found ${matching.size}.")
    val rollout = saved(matching.single().id)
    if (rollout.definition != definition || rollout.candidate != candidate.versionId) throw AuthorizationResourceNotFoundException()
    return rollout
  }

  fun requireCandidate(
    rollout: RolloutAuthorizationMetadata,
    request: RolloutCandidate,
  ) {
    if (rollout.definition != request.definition || rollout.repository != request.repository || rollout.tag != request.tag) {
      throw RolloutReferenceMismatchException("Rollout definition and candidate version do not match the requested connector.")
    }
  }

  fun requireActors(
    rollout: RolloutAuthorizationMetadata,
    actorIds: List<UUID>,
  ) {
    for (actor in actorIds) {
      val owner =
        try {
          data.actorWorkspaceAndOrganization(actor)
        } catch (_: AuthorizationResourceNotFoundException) {
          throw RolloutReferenceMismatchException("Rollout actor $actor does not exist.")
        }
      if (data.savedWorkspaceAndOrganization(owner.workspaceId, true) != owner) throw AuthorizationResourceNotFoundException()
      if (data.savedActorDefinitionId(actor, owner.workspaceId) !=
        rollout.definition
      ) {
        throw RolloutReferenceMismatchException("Rollout actor $actor belongs to another definition.")
      }
    }
  }

  private fun taggedVersion(
    definition: UUID,
    tag: String,
  ): ActorDefinitionVersion {
    val candidate =
      definitions.getActorDefinitionVersion(definition, tag).orElseThrow {
        RolloutReferenceMismatchException("Actor definition version not found for actor definition id: $definition and docker image tag: $tag")
      }
    if (candidate.actorDefinitionId != definition || candidate.versionId == null ||
      candidate.dockerImageTag != tag
    ) {
      throw AuthorizationResourceNotFoundException()
    }
    return candidate
  }

  private fun version(
    id: UUID,
    definition: UUID,
  ): ActorDefinitionVersion {
    val version =
      try {
        definitions.getActorDefinitionVersion(id)
      } catch (_: ConfigNotFoundException) {
        throw AuthorizationResourceNotFoundException()
      }
    if (version.versionId != id || version.actorDefinitionId != definition) throw AuthorizationResourceNotFoundException()
    return version
  }
}
