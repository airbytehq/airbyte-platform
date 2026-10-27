/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.ActorType
import io.airbyte.api.server.generated.models.ConnectionAndJobIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionAutoPropagateSchemaChange
import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionDataHistoryRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventsBackfillRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventsListMinimalRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventsRequestBody
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionLastJobPerStreamRequestBody
import io.airbyte.api.server.generated.models.ConnectionStatusesRequestBody
import io.airbyte.api.server.generated.models.ConnectionStreamHistoryRequestBody
import io.airbyte.api.server.generated.models.ConnectionStreamRefreshRequestBody
import io.airbyte.api.server.generated.models.ConnectionStreamRequestBody
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.ConnectionUpdateWithReason
import io.airbyte.api.server.generated.models.ConnectionUptimeHistoryRequestBody
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.PostprocessDiscoveredCatalogRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface ConnectionOperationInput : OperationInput {
  data class Saved(
    val ids: List<UUID>,
    val workspaceId: Optional<UUID> = Optional.empty(),
    val catalogId: Optional<UUID> = Optional.empty(),
    val jobId: Optional<Long> = Optional.empty(),
    val schemaChange: Boolean = false,
  ) : ConnectionOperationInput

  data class Workspaces(
    val ids: List<UUID>,
    val tagIds: List<UUID> = emptyList(),
  ) : ConnectionOperationInput

  data class Job(
    val id: Long,
  ) : ConnectionOperationInput

  data class Definition(
    val id: UUID,
    val source: Boolean,
  ) : ConnectionOperationInput
}

private fun savedConnectionInput(id: UUID?): ConnectionOperationInput.Saved = ConnectionOperationInput.Saved(listOf(id ?: throw BadRequestProblem()))

private fun connectionInput(args: Array<out Any?>): ConnectionOperationInput =
  when (val body = args.firstOrNull()) {
    is ConnectionIdRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionDataHistoryRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionLastJobPerStreamRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionUptimeHistoryRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionStreamHistoryRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionEventsRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionEventsBackfillRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionStreamRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionStreamRefreshRequestBody -> savedConnectionInput(body.connectionId)
    is ConnectionStatusesRequestBody -> ConnectionOperationInput.Saved(copyIds(body.connectionIds))
    is ConnectionAndJobIdRequestBody ->
      ConnectionOperationInput.Saved(
        listOf(body.connectionId),
        jobId =
          Optional.of(body.jobId),
      )
    is PostprocessDiscoveredCatalogRequestBody ->
      ConnectionOperationInput.Saved(
        listOf(body.connectionId),
        catalogId =
          Optional.of(body.catalogId),
        schemaChange = true,
      )
    is ConnectionAutoPropagateSchemaChange ->
      ConnectionOperationInput.Saved(
        listOf(body.connectionId),
        Optional.of(body.workspaceId),
        Optional.of(body.catalogId),
        schemaChange = true,
      )
    is JobIdRequestBody -> ConnectionOperationInput.Job(body.id)
    is WorkspaceIdRequestBody -> ConnectionOperationInput.Workspaces(listOf(body.workspaceId))
    is ListConnectionsForWorkspacesRequestBody ->
      ConnectionOperationInput.Workspaces(
        copyIds(body.workspaceIds),
        copyIds(body.tagIds),
      )
    is ActorDefinitionRequestBody ->
      ConnectionOperationInput.Definition(
        body.actorDefinitionId,
        when (body.actorType) {
          ActorType.SOURCE -> true
          ActorType.DESTINATION -> false
        },
      )
    is UUID -> savedConnectionInput(body)
    else -> throw BadRequestProblem()
  }

internal fun connectionOwner(
  data: AuthorizationDataService,
  connectionId: UUID,
): WorkspaceAndOrganization {
  val (sourceId, destinationId) = data.savedConnectionActorIds(connectionId)
  return connectionOwner(data, sourceId, destinationId)
}

internal fun connectionOwner(
  data: AuthorizationDataService,
  references: SavedConnectionReferences,
): WorkspaceAndOrganization = connectionOwner(data, references.sourceId, references.destinationId)

private fun connectionOwner(
  data: AuthorizationDataService,
  sourceId: UUID,
  destinationId: UUID,
): WorkspaceAndOrganization {
  val owner = data.savedSourceWorkspaceAndOrganization(sourceId)
  if (data.savedDestinationWorkspaceAndOrganization(destinationId) != owner) throw BadRequestProblem()
  return owner
}

internal fun requireSavedDestinationCatalog(
  data: AuthorizationDataService,
  connectionId: UUID,
  workspaceId: UUID,
  references: SavedConnectionReferences,
) {
  data.destinationCatalogForValidation(connectionId, workspaceId, references.destinationCatalogId).ifPresent { catalogId ->
    if (!data.catalogWasFetchedByActor(catalogId, references.destinationId)) throw BadRequestProblem()
  }
}

internal fun requireSavedSourceCatalog(
  data: AuthorizationDataService,
  references: SavedConnectionReferences,
) {
  references.sourceCatalogId.ifPresent { catalogId ->
    if (data.catalogWasFetchedByAnotherActor(catalogId, references.sourceId)) throw BadRequestProblem()
  }
}

abstract class ConnectionOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val global: Boolean = false,
  private val callers: Set<OperationCaller> = OperationCaller.entries.toSet(),
) : OperationRule {
  override fun readInput(args: Array<out Any?>): ConnectionOperationInput = connectionInput(args)

  protected open fun validateSavedConnection(
    owner: WorkspaceAndOrganization,
    references: SavedConnectionReferences,
  ) {}

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ConnectionOperationInput
    val savedReferences = if (request is ConnectionOperationInput.Saved) request.ids.map(data::savedConnectionReferences) else emptyList()
    val owners =
      when (request) {
        is ConnectionOperationInput.Saved -> savedReferences.map { connectionOwner(data, it) }
        is ConnectionOperationInput.Workspaces -> request.ids.map(data::workspaceAndOrganization)
        is ConnectionOperationInput.Job -> {
          if (request.id < 0) throw BadRequestProblem()
          listOf(connectionOwner(data, data.replicationJobConnectionId(request.id)))
        }
        is ConnectionOperationInput.Definition -> {
          data.requireActorDefinitionType(request.id, request.source)
          emptyList()
        }
      }
    val explicitEmpty =
      when (request) {
        is ConnectionOperationInput.Saved -> request.ids.isEmpty()
        is ConnectionOperationInput.Workspaces -> request.ids.isEmpty()
        else -> false
      }
    when {
      global || explicitEmpty -> permissions.requireGlobal(caller, roles, callers)
      else -> permissions.requireWorkspaces(caller, owners, roles, callers)
    }
    // Filters on an explicit empty target list cannot select another resource.
    if (explicitEmpty) return
    when (request) {
      is ConnectionOperationInput.Saved ->
        request.ids.forEachIndexed { index, id ->
          val owner = owners[index]
          val references = savedReferences[index]
          request.workspaceId.ifPresent { if (it != owner.workspaceId) throw BadRequestProblem() }
          request.catalogId.ifPresent { catalogId ->
            if (!data.catalogWasFetchedByActor(catalogId, references.sourceId)) throw BadRequestProblem()
          }
          if (request.schemaChange) {
            requireSavedSourceCatalog(data, references)
            requireSavedDestinationCatalog(data, id, owner.workspaceId, references)
          }
          request.jobId.ifPresent {
            if (it < 0) throw BadRequestProblem()
            if (data.replicationJobConnectionId(it) != id) throw BadRequestProblem()
          }
          validateSavedConnection(owner, references)
        }
      is ConnectionOperationInput.Workspaces -> for (tagId in request.tagIds) {
        if (data.tagWorkspaceAndOrganization(tagId) !in
          owners
        ) {
          throw BadRequestProblem()
        }
      }
      else -> Unit
    }
  }
}

@Singleton
class ConnectionReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(data, permissions, workspaceReadRoles)

@Singleton
class ConnectionDestinationCatalogReadRule(
  private val data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(data, permissions, workspaceReadRoles) {
  override fun validateSavedConnection(
    owner: WorkspaceAndOrganization,
    references: SavedConnectionReferences,
  ) {
    references.destinationCatalogId.ifPresent { catalogId ->
      if (!data.catalogWasFetchedByActor(catalogId, references.destinationId)) throw BadRequestProblem()
    }
  }
}

@Singleton
class ConnectionReadOrWorkerRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(
    data,
    permissions,
    workspaceReadRoles + AuthRoleConstants.DATAPLANE,
  )

@Singleton
class ConnectionWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(data, permissions, connectionWriteRoles)

@Singleton
class ConnectionRunRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(
    data,
    permissions,
    connectionWriteRoles +
      setOf(
        AuthRoleConstants.WORKSPACE_RUNNER,
        AuthRoleConstants.ORGANIZATION_RUNNER,
        AuthRoleConstants.WORKSPACE_ADMIN,
        AuthRoleConstants.ORGANIZATION_ADMIN,
        AuthRoleConstants.ADMIN,
      ),
  )

@Singleton
class ConnectionAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(data, permissions, setOf(AuthRoleConstants.ADMIN), global = true)

data class ConnectionEventsMinimalInput(
  val workspaceId: Optional<UUID>,
) : OperationInput

@Singleton
class ConnectionEventsMinimalRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): ConnectionEventsMinimalInput {
    val body = args.singleOrNull() as? ConnectionEventsListMinimalRequestBody ?: throw BadRequestProblem()
    return ConnectionEventsMinimalInput(Optional.ofNullable(body.workspaceId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val workspaceId = (input as ConnectionEventsMinimalInput).workspaceId
    if (workspaceId.isEmpty) {
      // The handler returns an empty response without reading any workspace when this field is absent.
      permissions.requireAuthenticated(caller, OperationCaller.entries.toSet())
      return
    }
    val owner = data.workspaceAndOrganization(workspaceId.get())
    permissions.requireWorkspaces(caller, listOf(owner), workspaceReadRoles, OperationCaller.entries.toSet())
  }
}

@Singleton
class FusionConnectionReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(
    data,
    permissions,
    setOf(AuthRoleConstants.WORKSPACE_READER),
    callers = OperationCaller.entries.toSet(),
  )

@Singleton
class FusionConnectionWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(
    data,
    permissions,
    setOf(AuthRoleConstants.WORKSPACE_EDITOR),
    callers = OperationCaller.entries.toSet(),
  )

@Singleton
class FusionConnectionBackfillRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(
    data,
    permissions,
    setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_RUNNER, AuthRoleConstants.ORGANIZATION_EDITOR),
    callers = OperationCaller.entries.toSet(),
  )

data class ConnectionMutationReferences(
  val operationIds: List<UUID>,
  val sourceCatalogId: Optional<UUID>,
  val destinationCatalogId: Optional<UUID>,
  val dataplaneGroupId: Optional<UUID>,
  val tagIds: List<UUID>,
)

sealed interface ConnectionMutationInput : OperationInput {
  val references: ConnectionMutationReferences

  data class Create(
    val sourceId: UUID,
    val destinationId: UUID,
    override val references: ConnectionMutationReferences,
  ) : ConnectionMutationInput

  data class Update(
    val connectionId: UUID,
    override val references: ConnectionMutationReferences,
    val hasSyncCatalog: Boolean,
  ) : ConnectionMutationInput
}

@Singleton
class ConnectionMutationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): ConnectionMutationInput =
    when (val body = args.singleOrNull()) {
      is ConnectionCreate ->
        ConnectionMutationInput.Create(
          body.sourceId,
          body.destinationId,
          ConnectionMutationReferences(
            copyIds(body.operationIds),
            Optional.ofNullable(body.sourceCatalogId),
            Optional.ofNullable(body.destinationCatalogId),
            Optional.ofNullable(body.dataplaneGroupId),
            body.tags
              .orEmpty()
              .map { it.tagId }
              .toList(),
          ),
        )
      is ConnectionUpdate -> updateInput(body)
      is ConnectionUpdateWithReason -> updateInput(body.connectionUpdate)
      else -> throw BadRequestProblem()
    }

  private fun updateInput(body: ConnectionUpdate): ConnectionMutationInput.Update =
    ConnectionMutationInput.Update(
      body.connectionId,
      ConnectionMutationReferences(
        copyIds(body.operationIds),
        Optional.ofNullable(body.sourceCatalogId),
        Optional.ofNullable(body.destinationCatalogId),
        Optional.ofNullable(body.dataplaneGroupId),
        body.tags
          .orEmpty()
          .map { it.tagId }
          .toList(),
      ),
      body.syncCatalog != null,
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ConnectionMutationInput
    val (sourceId, destinationId, savedDestinationCatalogId) =
      when (request) {
        is ConnectionMutationInput.Create -> Triple(request.sourceId, request.destinationId, Optional.empty())
        is ConnectionMutationInput.Update ->
          data.savedConnectionReferences(request.connectionId).let {
            Triple(it.sourceId, it.destinationId, it.destinationCatalogId)
          }
      }
    val owner = data.savedSourceWorkspaceAndOrganization(sourceId)
    permissions.requireWorkspaces(caller, listOf(owner), connectionWriteRoles, OperationCaller.entries.toSet())
    if (data.savedDestinationWorkspaceAndOrganization(destinationId) != owner) throw BadRequestProblem()
    val refs = request.references
    for (id in refs.operationIds) if (data.operationWorkspaceAndOrganization(id) != owner) throw BadRequestProblem()
    refs.sourceCatalogId.ifPresent { if (!data.catalogWasFetchedByActor(it, sourceId)) throw BadRequestProblem() }
    val destinationCatalogId =
      if (refs.destinationCatalogId.isEmpty && request is ConnectionMutationInput.Update && request.hasSyncCatalog) {
        data.destinationCatalogForValidation(request.connectionId, owner.workspaceId, savedDestinationCatalogId)
      } else {
        refs.destinationCatalogId
      }
    destinationCatalogId.ifPresent { if (!data.catalogWasFetchedByActor(it, destinationId)) throw BadRequestProblem() }
    refs.dataplaneGroupId.ifPresent { if (!data.canUseDataplaneGroup(it, owner.organizationId)) throw BadRequestProblem() }
    for (id in refs.tagIds) if (data.tagWorkspaceAndOrganization(id) != owner) throw BadRequestProblem()
  }
}

@Singleton
class ConnectionDeleteRule(
  private val data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ConnectionOperationRule(data, permissions, connectionWriteRoles) {
  override fun validateSavedConnection(
    owner: WorkspaceAndOrganization,
    references: SavedConnectionReferences,
  ) {
    for (operationId in references.operationIds) {
      if (data.operationWorkspaceAndOrganization(operationId) != owner) throw BadRequestProblem()
    }
  }
}
