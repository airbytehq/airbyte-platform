/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.CompleteDestinationOAuthRequest
import io.airbyte.api.server.generated.models.CompleteSourceOauthRequest
import io.airbyte.api.server.generated.models.DestinationOauthConsentRequest
import io.airbyte.api.server.generated.models.EmbeddedSourceOauthConsentRequest
import io.airbyte.api.server.generated.models.RevokeSourceOauthTokensRequest
import io.airbyte.api.server.generated.models.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.api.server.generated.models.SetInstancewideSourceOauthParamsRequestBody
import io.airbyte.api.server.generated.models.SourceOAuthScopesRequest
import io.airbyte.api.server.generated.models.SourceOauthConsentRequest
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class OAuthFlowInput(
  val workspace: UUID,
  val definition: UUID,
  val actor: Optional<UUID>,
) : OperationInput

data class OAuthInstanceInput(
  val definition: UUID,
) : OperationInput

abstract class OAuthFlowRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is CompleteSourceOauthRequest -> flow(body.workspaceId, body.sourceDefinitionId, Optional.ofNullable(body.sourceId))
      is SourceOauthConsentRequest -> flow(body.workspaceId, body.sourceDefinitionId, Optional.ofNullable(body.sourceId))
      is EmbeddedSourceOauthConsentRequest -> flow(body.workspaceId, body.sourceDefinitionId, Optional.ofNullable(body.sourceId))
      is RevokeSourceOauthTokensRequest -> flow(body.workspaceId, body.sourceDefinitionId, Optional.of(body.sourceId))
      is SourceOAuthScopesRequest -> flow(body.workspaceId, body.sourceDefinitionId, Optional.empty())
      is CompleteDestinationOAuthRequest -> flow(body.workspaceId, body.destinationDefinitionId, Optional.ofNullable(body.destinationId))
      is DestinationOauthConsentRequest -> flow(body.workspaceId, body.destinationDefinitionId, Optional.ofNullable(body.destinationId))
      else -> throw BadRequestProblem()
    }

  private fun flow(
    workspace: UUID?,
    definition: UUID?,
    actor: Optional<UUID>,
  ) = OAuthFlowInput(
    workspace ?: throw BadRequestProblem(),
    definition ?: throw BadRequestProblem(),
    actor,
  )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as OAuthFlowInput
    val owner = data.workspaceAndOrganization(request.workspace)
    permissions.requireWorkspaces(
      caller,
      listOf(owner),
      if (source) Intent.RunOAuthFlow.roles else destinationWriteRoles,
      if (source) organizationCallers + OperationCaller.EMBEDDED else organizationCallers,
    )
    data.requireActorDefinitionType(request.definition, source)
    if (!data.canUseActorDefinition(request.definition, owner)) throw ForbiddenProblem()
    request.actor.ifPresent { actor ->
      val saved = if (source) data.savedSourceWorkspaceAndOrganization(actor) else data.savedDestinationWorkspaceAndOrganization(actor)
      if (saved != owner || data.savedActorDefinitionId(actor, owner.workspaceId) != request.definition) throw BadRequestProblem()
    }
  }
}

@Singleton
class SourceOAuthFlowRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OAuthFlowRule(data, permissions, true)

@Singleton
class DestinationOAuthFlowRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OAuthFlowRule(data, permissions, false)

abstract class OAuthInstanceRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    OAuthInstanceInput(
      when (val body = args.singleOrNull()) {
        is SetInstancewideSourceOauthParamsRequestBody -> body.sourceDefinitionId
        is SetInstancewideDestinationOauthParamsRequestBody -> body.destinationDefinitionId
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
    data.requireActorDefinitionType((input as OAuthInstanceInput).definition, source)
  }
}

@Singleton
class SourceOAuthInstanceRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OAuthInstanceRule(data, permissions, true)

@Singleton
class DestinationOAuthInstanceRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : OAuthInstanceRule(data, permissions, false)
