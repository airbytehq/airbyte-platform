/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.model.generated.RunCheckCommandRequest
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.json.Jsons
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface CheckCommandInput : OperationInput {
  data class Saved(
    val actorId: UUID,
    val suppliedWorkspaceId: Optional<UUID>,
    val suppliedDefinitionId: Optional<UUID>,
  ) : CheckCommandInput

  data class Inline(
    val workspaceId: UUID,
    val actorDefinitionId: UUID,
  ) : CheckCommandInput
}

@Singleton
class CheckCommandRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): CheckCommandInput {
    val body = args.singleOrNull() as? RunCheckCommandRequest ?: throw BadRequestProblem()
    val id: String? = body.id
    if (id.isNullOrBlank()) throw BadRequestProblem()
    if (body.config != null && !Jsons.jsonNode(body.config).isObject) throw BadRequestProblem()
    if (body.attemptNumber != null && body.attemptNumber < 0) throw BadRequestProblem()
    if (body.jobId != null && body.jobId.isBlank()) throw BadRequestProblem()

    val actorId: UUID? = body.actorId
    if (actorId != null) return CheckCommandInput.Saved(actorId, Optional.ofNullable(body.workspaceId), Optional.ofNullable(body.actorDefinitionId))
    if (body.config == null) throw BadRequestProblem()
    return CheckCommandInput.Inline(body.workspaceId ?: throw BadRequestProblem(), body.actorDefinitionId ?: throw BadRequestProblem())
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as CheckCommandInput
    val owner =
      when (request) {
        is CheckCommandInput.Saved -> data.actorWorkspaceAndOrganization(request.actorId)
        is CheckCommandInput.Inline -> data.workspaceAndOrganization(request.workspaceId)
      }
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      setOf(AuthRoleConstants.WORKSPACE_RUNNER, AuthRoleConstants.EMBEDDED_END_USER),
      OperationCaller.entries.toSet(),
    )

    when (request) {
      is CheckCommandInput.Saved -> {
        request.suppliedWorkspaceId.ifPresent { if (it != owner.workspaceId) throw BadRequestProblem() }
        request.suppliedDefinitionId.ifPresent { definitionId ->
          val savedDefinitionId = data.savedActorDefinitionId(request.actorId, owner.workspaceId)
          if (definitionId != savedDefinitionId) throw BadRequestProblem()
        }
      }
      is CheckCommandInput.Inline -> if (!data.canUseActorDefinition(request.actorDefinitionId, owner)) throw ForbiddenProblem()
    }
    // JobInputService.getCheckInput uses job/attempt values for tracking, without selecting a saved job.
  }
}
