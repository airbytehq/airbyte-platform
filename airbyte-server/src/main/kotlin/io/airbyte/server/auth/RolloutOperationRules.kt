/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ConnectorRolloutInvalidRequestProblem
import io.airbyte.api.server.generated.models.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutGetActorSyncInfoRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutListByActorDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutListRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutReadRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutStartRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.RolloutAuthorizationDataService
import io.airbyte.domain.services.auth.RolloutCandidate
import io.airbyte.domain.services.auth.RolloutReferenceMismatchException
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface RolloutSelector {
  data class Saved(
    val id: UUID,
  ) : RolloutSelector

  data class DefinitionAndTag(
    val definition: UUID,
    val tag: String,
  ) : RolloutSelector
}

data class RolloutOperationInput(
  val selector: RolloutSelector,
  val actors: List<UUID> = emptyList(),
  val candidate: Optional<RolloutCandidate> = Optional.empty(),
) : OperationInput

data class RolloutStartInput(
  val candidate: RolloutCandidate,
) : OperationInput

data class RolloutFilterInput(
  val definition: UUID,
  val tag: Optional<String>,
) : OperationInput

private fun rolloutId(id: UUID?): RolloutSelector = RolloutSelector.Saved(id ?: throw BadRequestProblem())

private fun rolloutActors(actors: List<UUID>?): List<UUID> = actors.orEmpty()

private fun rolloutCandidate(
  definition: UUID?,
  repository: String?,
  tag: String?,
) = RolloutCandidate(
  requireNotNull(definition),
  requireNotNull(repository),
  requireNotNull(tag),
)

abstract class RolloutOperationRule(
  protected val data: RolloutAuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  protected fun requireAdmin(
    caller: Authentication,
    worker: Boolean = false,
  ) = permissions.requireGlobal(
    caller,
    setOf(AuthRoleConstants.ADMIN),
    if (worker) setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT) else organizationCallers,
  )

  protected fun references(input: RolloutOperationInput) {
    try {
      val rollout =
        when (val selected = input.selector) {
          is RolloutSelector.Saved -> data.saved(selected.id)
          is RolloutSelector.DefinitionAndTag -> data.byDefinitionAndTag(selected.definition, selected.tag)
        }
      input.candidate.ifPresent { data.requireCandidate(rollout, it) }
      data.requireActors(rollout, input.actors)
    } catch (failure: RolloutReferenceMismatchException) {
      throw ConnectorRolloutInvalidRequestProblem(ProblemMessageData().message(failure.message))
    }
  }
}

@Singleton
class RolloutWorkerRule(
  data: RolloutAuthorizationDataService,
  permissions: OperationPermissions,
) : RolloutOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is ConnectorRolloutStartRequestBody -> RolloutOperationInput(rolloutId(body.id))
      is ConnectorRolloutRequestBody -> RolloutOperationInput(rolloutId(body.id), rolloutActors(body.actorIds))
      is ConnectorRolloutFinalizeRequestBody -> RolloutOperationInput(rolloutId(body.id))
      is ConnectorRolloutUpdateStateRequestBody ->
        RolloutOperationInput(
          body.id?.let(::rolloutId) ?: RolloutSelector.DefinitionAndTag(
            requireNotNull(body.actorDefinitionId),
            requireNotNull(body.dockerImageTag),
          ),
        )
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller, worker = true)
    references(input as RolloutOperationInput)
  }
}

@Singleton
class RolloutReadRule(
  data: RolloutAuthorizationDataService,
  permissions: OperationPermissions,
) : RolloutOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    RolloutOperationInput(
      when (val body = args.singleOrNull()) {
        is ConnectorRolloutReadRequestBody -> rolloutId(body.id)
        is ConnectorRolloutGetActorSyncInfoRequestBody -> rolloutId(body.id)
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    references(input as RolloutOperationInput)
  }
}

@Singleton
class RolloutFilterRule(
  data: RolloutAuthorizationDataService,
  permissions: OperationPermissions,
) : RolloutOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is ConnectorRolloutListRequestBody ->
        RolloutFilterInput(
          body.actorDefinitionId,
          Optional.ofNullable(body.dockerImageTag),
        )
      is ConnectorRolloutListByActorDefinitionIdRequestBody ->
        RolloutFilterInput(
          body.actorDefinitionId,
          Optional.empty(),
        )
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = requireAdmin(caller)
}

@Singleton
class RolloutManualStartRule(
  data: RolloutAuthorizationDataService,
  permissions: OperationPermissions,
) : RolloutOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ConnectorRolloutManualStartRequestBody ?: throw BadRequestProblem()
    return RolloutStartInput(rolloutCandidate(body.actorDefinitionId, body.dockerRepository, body.dockerImageTag))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    try {
      data.candidate((input as RolloutStartInput).candidate)
    } catch (failure: RolloutReferenceMismatchException) {
      throw ConnectorRolloutInvalidRequestProblem(ProblemMessageData().message(failure.message))
    }
  }
}

@Singleton
class RolloutManualChangeRule(
  data: RolloutAuthorizationDataService,
  permissions: OperationPermissions,
) : RolloutOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is ConnectorRolloutManualRolloutRequestBody ->
        RolloutOperationInput(
          rolloutId(body.id),
          rolloutActors(body.actorIds),
          Optional.of(
            rolloutCandidate(
              body.actorDefinitionId,
              body.dockerRepository,
              body.dockerImageTag,
            ),
          ),
        )
      is ConnectorRolloutManualFinalizeRequestBody ->
        RolloutOperationInput(
          rolloutId(body.id),
          candidate =
            Optional.of(
              rolloutCandidate(
                body.actorDefinitionId,
                body.dockerRepository,
                body.dockerImageTag,
              ),
            ),
        )
      is ConnectorRolloutUpdateStateRequestBody ->
        RolloutOperationInput(
          rolloutId(body.id),
          candidate =
            Optional.of(
              rolloutCandidate(
                body.actorDefinitionId,
                body.dockerRepository,
                body.dockerImageTag,
              ),
            ),
        )
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    references(input as RolloutOperationInput)
  }
}
