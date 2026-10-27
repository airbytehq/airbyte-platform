/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.DestinationDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.SourceDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

sealed interface DefinitionRegistryInput : OperationInput {
  data class Definition(
    val id: UUID,
  ) : DefinitionRegistryInput

  data class Workspace(
    val id: UUID,
  ) : DefinitionRegistryInput

  data class Scoped(
    val definition: UUID,
    val scope: UUID,
    val organization: Boolean,
  ) : DefinitionRegistryInput
}

private fun registryInput(args: Array<out Any?>): DefinitionRegistryInput =
  when (val body = args.singleOrNull()) {
    is SourceDefinitionIdRequestBody -> DefinitionRegistryInput.Definition(body.sourceDefinitionId)
    is DestinationDefinitionIdRequestBody -> DefinitionRegistryInput.Definition(body.destinationDefinitionId)
    is SourceDefinitionIdWithWorkspaceId -> DefinitionRegistryInput.Scoped(body.sourceDefinitionId, body.workspaceId, false)
    is DestinationDefinitionIdWithWorkspaceId -> DefinitionRegistryInput.Scoped(body.destinationDefinitionId, body.workspaceId, false)
    is ActorDefinitionIdWithScope -> DefinitionRegistryInput.Scoped(body.actorDefinitionId, body.scopeId, body.scopeType == ScopeType.ORGANIZATION)
    is WorkspaceIdRequestBody -> DefinitionRegistryInput.Workspace(body.workspaceId)
    is WorkspaceIdActorDefinitionRequestBody -> DefinitionRegistryInput.Workspace(body.workspaceId)
    else -> throw BadRequestProblem()
  }

private val registryCallers = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

abstract class DefinitionRegistryMetadataRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = registryInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, registryCallers)
    val definition = (input as DefinitionRegistryInput.Definition).id
    data.requireActorDefinitionType(definition, source)
    data.requireDefinitionDefaultVersion(definition)
  }
}

@Singleton
class SourceRegistryMetadataRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryMetadataRule(data, permissions, true)

@Singleton
class DestinationRegistryMetadataRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryMetadataRule(data, permissions, false)

@Singleton
class DefinitionRegistryWorkspaceReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = registryInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireWorkspaces(
      caller,
      listOf(data.savedWorkspaceAndOrganization((input as DefinitionRegistryInput.Workspace).id, true)),
      workspaceReadRoles,
      registryCallers,
    )
  }
}

/** The supplied workspace only filters grant availability in this instance-admin registry list. */
@Singleton
class DefinitionRegistryPrivateListRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = registryInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), registryCallers)
  }
}

abstract class DefinitionRegistryScopedReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = registryInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as DefinitionRegistryInput.Scoped
    val usable =
      if (request.organization) {
        permissions.requireOrganizations(caller, listOf(data.organization(request.scope)), workspaceReadRoles, registryCallers)
        data.requireActorDefinitionType(request.definition, source)
        data.organizationCanUseActorDefinition(request.definition, request.scope)
      } else {
        val owner = data.workspaceAndOrganization(request.scope)
        permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, registryCallers)
        data.requireActorDefinitionType(request.definition, source)
        data.canUseActorDefinition(request.definition, owner)
      }
    if (!usable) {
      throw IdNotFoundKnownException(
        "Cannot find the requested definition with given id for this ${if (request.organization) "organization" else "workspace"}",
        request.definition.toString(),
      )
    }
  }
}

@Singleton
class SourceRegistryScopedReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryScopedReadRule(data, permissions, true)

@Singleton
class DestinationRegistryScopedReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryScopedReadRule(data, permissions, false)

abstract class DefinitionRegistryGrantRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = registryInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), registryCallers)
    val request = input as DefinitionRegistryInput.Scoped
    if (request.organization) data.organization(request.scope) else data.workspaceAndOrganization(request.scope)
    data.requireActorDefinitionType(request.definition, source)
    data.requireDefinitionDefaultVersion(request.definition)
  }
}

@Singleton
class SourceRegistryGrantRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryGrantRule(data, permissions, true)

@Singleton
class DestinationRegistryGrantRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryGrantRule(data, permissions, false)

abstract class DefinitionRegistryRevokeRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = registryInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), registryCallers)
    data.requireActorDefinitionTypeIfPresent((input as DefinitionRegistryInput.Scoped).definition, source)
  }
}

@Singleton
class SourceRegistryRevokeRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryRevokeRule(data, permissions, true)

@Singleton
class DestinationRegistryRevokeRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DefinitionRegistryRevokeRule(data, permissions, false)
