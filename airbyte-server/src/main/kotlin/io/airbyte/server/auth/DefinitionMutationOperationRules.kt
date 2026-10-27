/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.api.server.generated.models.CustomDestinationDefinitionCreate
import io.airbyte.api.server.generated.models.CustomSourceDefinitionCreate
import io.airbyte.api.server.generated.models.DestinationDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.DestinationDefinitionUpdate
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.SourceDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.SourceDefinitionUpdate
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface CustomDefinitionCreateInput : OperationInput {
  data class Workspace(
    val id: UUID,
  ) : CustomDefinitionCreateInput

  data class UnsupportedOrganization(
    val organization: Optional<UUID>,
    val message: String,
  ) : CustomDefinitionCreateInput
}

private val definitionMutationCallers =
  setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

@Singleton
class CustomDefinitionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is CustomSourceDefinitionCreate ->
        creationInput(
          Optional.ofNullable(body.workspaceId),
          Optional.ofNullable(body.scopeId),
          Optional.ofNullable(body.scopeType),
          "Cannot determine workspace ID for custom source definition creation: $body",
        )
      is CustomDestinationDefinitionCreate ->
        creationInput(
          Optional.ofNullable(body.workspaceId),
          Optional.ofNullable(body.scopeId),
          Optional.ofNullable(body.scopeType),
          "Cannot determine workspace ID for custom destination definition creation: $body",
        )
      else -> throw BadRequestProblem()
    }

  private fun creationInput(
    workspace: Optional<UUID>,
    scope: Optional<UUID>,
    type: Optional<ScopeType>,
    error: String,
  ): CustomDefinitionCreateInput {
    if (workspace.isPresent) return CustomDefinitionCreateInput.Workspace(workspace.orElseThrow())
    return when (type.orElseThrow { IllegalArgumentException("Unexpected value 'null'") }) {
      ScopeType.WORKSPACE -> CustomDefinitionCreateInput.Workspace(scope.orElseThrow { NullPointerException() })
      ScopeType.ORGANIZATION -> CustomDefinitionCreateInput.UnsupportedOrganization(scope, error)
    }
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    when (val request = input as CustomDefinitionCreateInput) {
      is CustomDefinitionCreateInput.Workspace ->
        permissions.requireWorkspaces(
          caller,
          listOf(data.workspaceAndOrganization(request.id)),
          Intent.UploadCustomConnector.roles,
          definitionMutationCallers,
        )
      is CustomDefinitionCreateInput.UnsupportedOrganization -> {
        // The existing intent still applies, but this unsupported body consumes no saved organization.
        if (request.organization.isPresent) {
          permissions.requireOrganizations(
            caller,
            listOf(request.organization.orElseThrow()),
            Intent.UploadCustomConnector.roles,
            definitionMutationCallers,
          )
        } else {
          permissions.requireGlobal(caller, Intent.UploadCustomConnector.roles, definitionMutationCallers)
        }
        throw UnprocessableEntityProblem(ProblemMessageData().message(request.message))
      }
    }
  }
}

sealed interface DefinitionMutationInput : OperationInput {
  val definition: UUID

  data class Delete(
    override val definition: UUID,
  ) : DefinitionMutationInput

  data class Update(
    override val definition: UUID,
    val workspace: UUID,
  ) : DefinitionMutationInput
}

/** Community retains its authentication-only definition write contract. */
abstract class DefinitionMutationRule(
  protected val data: AuthorizationDataService,
  protected val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is SourceDefinitionUpdate -> DefinitionMutationInput.Update(body.sourceDefinitionId, body.workspaceId)
      is DestinationDefinitionUpdate -> DefinitionMutationInput.Update(body.destinationDefinitionId, body.workspaceId)
      is SourceDefinitionIdRequestBody -> DefinitionMutationInput.Delete(body.sourceDefinitionId)
      is DestinationDefinitionIdRequestBody -> DefinitionMutationInput.Delete(body.destinationDefinitionId)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as DefinitionMutationInput
    when (request) {
      is DefinitionMutationInput.Delete -> {
        permissions.requireAuthenticated(caller, definitionMutationCallers)
        data.requireActorDefinitionType(request.definition, source)
      }
      is DefinitionMutationInput.Update -> {
        val owner = data.workspaceAndOrganization(request.workspace)
        permissions.requireAuthenticated(caller, definitionMutationCallers, listOf(owner))
        data.requireActorDefinitionType(request.definition, source)
        data.requireDefinitionDefaultVersion(request.definition)
        if (!data.canUseActorDefinition(
            request.definition,
            owner,
          )
        ) {
          throw IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", request.definition.toString())
        }
      }
    }
  }

  protected fun requireInstanceAdmin(
    caller: Authentication,
    input: DefinitionMutationInput,
  ) {
    when (input) {
      is DefinitionMutationInput.Delete -> permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), definitionMutationCallers)
      is DefinitionMutationInput.Update ->
        permissions.requireWorkspaces(
          caller,
          listOf(data.workspaceAndOrganization(input.workspace)),
          setOf(AuthRoleConstants.ADMIN),
          definitionMutationCallers,
        )
    }
  }
}

@Singleton
class SourceDefinitionMutationRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionMutationRule(data, permissions, true)

@Singleton
class DestinationDefinitionMutationRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionMutationRule(data, permissions, false)
