/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionCreate
import io.airbyte.api.server.generated.models.WebBackendConnectionListRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionUpdate
import io.airbyte.api.server.generated.models.WebBackendValidateMappersRequestBody
import io.airbyte.api.server.generated.models.WebBackendWorkspaceState
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class WebBackendConnectionInput(
  val connectionId: UUID,
  val refreshCatalog: Boolean = false,
) : OperationInput

@Singleton
class WebBackendStateTypeRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendConnectionInput {
    val body = args.singleOrNull() as? ConnectionIdRequestBody ?: throw BadRequestProblem()
    return WebBackendConnectionInput(body.connectionId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = connectionOwner(data, (input as WebBackendConnectionInput).connectionId)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
  }
}

@Singleton
class WebBackendMappersRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendConnectionInput {
    val body = args.singleOrNull() as? WebBackendValidateMappersRequestBody ?: throw BadRequestProblem()
    return WebBackendConnectionInput(body.connectionId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = connectionOwner(data, (input as WebBackendConnectionInput).connectionId)
    permissions.requireWorkspaces(caller, listOf(owner), connectionWriteRoles, organizationCallers)
  }
}

private fun requireWebBackendActorDefinitions(
  data: AuthorizationDataService,
  source: UUID,
  destination: UUID,
  workspaceId: UUID,
) {
  data.requireActorDefinitionType(data.savedActorDefinitionId(source, workspaceId), true)
  data.requireActorDefinitionType(data.savedActorDefinitionId(destination, workspaceId), false)
}

private fun requireWebBackendOperations(
  data: AuthorizationDataService,
  connectionId: UUID,
  owner: WorkspaceAndOrganization,
): Set<UUID> {
  val ids = data.savedConnectionOperationIds(connectionId).toSet()
  for (id in ids) if (data.operationWorkspaceAndOrganization(id) != owner) throw BadRequestProblem()
  return ids
}

@Singleton
class WebBackendConnectionGetRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val currentUser: CurrentUserService,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendConnectionInput {
    val body = args.singleOrNull() as? WebBackendConnectionRequestBody ?: throw BadRequestProblem()
    return WebBackendConnectionInput(body.connectionId, body.withRefreshedCatalog == true)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WebBackendConnectionInput
    val references = data.savedConnectionReferences(request.connectionId)
    val owner = connectionOwner(data, references)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
    if (request.refreshCatalog) {
      permissions.requireWorkspaces(
        permissions.currentUser(caller, currentUser),
        listOf(owner),
        connectionWriteRoles,
        setOf(OperationCaller.USER),
      )
    }
    requireWebBackendOperations(data, request.connectionId, owner)
    requireSavedSourceCatalog(data, references)
    requireWebBackendActorDefinitions(data, references.sourceId, references.destinationId, owner.workspaceId)
  }
}

data class WebBackendWorkspaceInput(
  val workspaceId: UUID,
) : OperationInput

@Singleton
class WebBackendWorkspaceReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendWorkspaceInput =
    WebBackendWorkspaceInput(
      when (val body = args.singleOrNull()) {
        is WorkspaceIdRequestBody -> body.workspaceId
        is WebBackendWorkspaceState -> body.workspaceId
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owner = data.workspaceAndOrganization((input as WebBackendWorkspaceInput).workspaceId)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
  }
}

data class WebBackendConnectionListInput(
  val workspaceId: UUID,
  val sourceIds: List<UUID>,
  val destinationIds: List<UUID>,
) : OperationInput

@Singleton
class WebBackendConnectionListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendConnectionListInput {
    val body = args.singleOrNull() as? WebBackendConnectionListRequestBody ?: throw BadRequestProblem()
    return WebBackendConnectionListInput(body.workspaceId, copyIds(body.sourceId), copyIds(body.destinationId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WebBackendConnectionListInput
    val owner = data.workspaceAndOrganization(request.workspaceId)
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, organizationCallers)
    for (sourceId in request.sourceIds) if (data.savedSourceWorkspaceAndOrganization(sourceId) != owner) throw BadRequestProblem()
    for (destinationId in request.destinationIds) if (data.savedDestinationWorkspaceAndOrganization(destinationId) != owner) throw BadRequestProblem()
  }
}

data class WebBackendNestedOperationInput(
  val operationId: Optional<UUID>,
  val workspaceId: UUID,
)

data class WebBackendConnectionCreateInput(
  val mutation: ConnectionMutationInput.Create,
  val operationWorkspaceIds: List<UUID>,
) : OperationInput

@Singleton
class WebBackendConnectionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val mutations: ConnectionMutationRule,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendConnectionCreateInput {
    val body = args.singleOrNull() as? WebBackendConnectionCreate ?: throw BadRequestProblem()
    return WebBackendConnectionCreateInput(
      ConnectionMutationInput.Create(
        body.sourceId,
        body.destinationId,
        ConnectionMutationReferences(
          emptyList(), // The handler replaces operationIds with IDs created from the nested operations.
          Optional.ofNullable(body.sourceCatalogId),
          Optional.ofNullable(body.destinationCatalogId),
          Optional.ofNullable(body.dataplaneGroupId),
          body.tags
            .orEmpty()
            .map { it.tagId }
            .toList(),
        ),
      ),
      body.operations
        .orEmpty()
        .map { it.workspaceId }
        .toList(),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WebBackendConnectionCreateInput
    val owner = data.savedSourceWorkspaceAndOrganization(request.mutation.sourceId)
    permissions.requireAuthenticated(caller, organizationCallers, listOf(owner))
    mutations.authorize(caller, request.mutation)
    for (workspaceId in request.operationWorkspaceIds) if (data.workspaceAndOrganization(workspaceId) != owner) throw BadRequestProblem()
    requireWebBackendActorDefinitions(data, request.mutation.sourceId, request.mutation.destinationId, owner.workspaceId)
  }
}

data class WebBackendConnectionUpdateInput(
  val mutation: ConnectionMutationInput.Update,
  val operations: List<WebBackendNestedOperationInput>,
) : OperationInput

@Singleton
class WebBackendConnectionUpdateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val mutations: ConnectionMutationRule,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WebBackendConnectionUpdateInput {
    val body = args.singleOrNull() as? WebBackendConnectionUpdate ?: throw BadRequestProblem()
    return WebBackendConnectionUpdateInput(
      ConnectionMutationInput.Update(
        body.connectionId,
        ConnectionMutationReferences(
          emptyList(),
          Optional.ofNullable(body.sourceCatalogId),
          Optional.ofNullable(body.destinationCatalogId),
          Optional.ofNullable(body.dataplaneGroupId),
          body.tags
            .orEmpty()
            .map { it.tagId }
            .toList(),
        ),
        body.syncCatalog != null,
      ),
      body.operations
        .orEmpty()
        .map {
          WebBackendNestedOperationInput(Optional.ofNullable(it.operationId), it.workspaceId)
        }.toList(),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WebBackendConnectionUpdateInput
    val connectionId = request.mutation.connectionId
    val references = data.savedConnectionReferences(connectionId)
    val owner = connectionOwner(data, references)
    permissions.requireAuthenticated(caller, organizationCallers, listOf(owner))
    mutations.authorize(caller, request.mutation)
    val savedOperations = requireWebBackendOperations(data, connectionId, owner)
    for (operation in request.operations) {
      // Existing updates discard the supplied workspace; new operations consume it and replace the supplied ID.
      if (operation.operationId.isEmpty || operation.operationId.get() !in savedOperations) {
        if (data.workspaceAndOrganization(operation.workspaceId) != owner) throw BadRequestProblem()
      }
    }
    if (request.mutation.references.sourceCatalogId.isEmpty) requireSavedSourceCatalog(data, references)
    requireWebBackendActorDefinitions(data, references.sourceId, references.destinationId, owner.workspaceId)
  }
}
