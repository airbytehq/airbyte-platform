/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.ActorListCursorPaginatedRequestBody
import io.airbyte.api.server.generated.models.DestinationCreate
import io.airbyte.api.server.generated.models.DestinationDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.DestinationDiscoverSchemaWriteRequestBody
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.DestinationUpdate
import io.airbyte.api.server.generated.models.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.PartialDestinationUpdate
import io.airbyte.api.server.generated.models.PartialSourceUpdate
import io.airbyte.api.server.generated.models.SourceAutoPropagateChange
import io.airbyte.api.server.generated.models.SourceCreate
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceIdWithSecretCoordinatesRequestBody
import io.airbyte.api.server.generated.models.SourceUpdate
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface ActorOperationInput : OperationInput {
  data class Saved(
    val actorId: UUID,
    val workspaceId: Optional<UUID> = Optional.empty(),
    val connectionId: Optional<UUID> = Optional.empty(),
    val catalogId: Optional<UUID> = Optional.empty(),
    val hasSchemaCatalog: Boolean = false,
    val comparesSavedSourceCatalog: Boolean = false,
  ) : ActorOperationInput

  data class Create(
    val workspaceId: UUID,
    val definitionId: UUID,
  ) : ActorOperationInput

  data class Workspaces(
    val ids: List<UUID>,
  ) : ActorOperationInput
}

internal val workspaceReadRoles = setOf(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
internal val sourceWriteRoles =
  setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
internal val destinationWriteRoles =
  setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
internal val connectionWriteRoles = sourceWriteRoles + destinationWriteRoles

/** Common ownership checks; each declared rule supplies its own action and typed input reader. */
abstract class ActorOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
  private val roles: Set<String>,
  private val callers: Set<OperationCaller> = OperationCaller.entries.toSet(),
) : OperationRule {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as ActorOperationInput
    when (request) {
      is ActorOperationInput.Saved -> {
        val owner =
          if (source) {
            data.savedSourceWorkspaceAndOrganization(
              request.actorId,
            )
          } else {
            data.savedDestinationWorkspaceAndOrganization(request.actorId)
          }
        permissions.requireWorkspaces(caller, listOf(owner), roles, callers)
        request.workspaceId.ifPresent { if (it != owner.workspaceId) throw BadRequestProblem() }
        request.connectionId.ifPresent { id ->
          val references = data.savedConnectionReferences(id)
          val sourceId = references.sourceId
          val destinationId = references.destinationId
          if (request.actorId != if (source) sourceId else destinationId) throw BadRequestProblem()
          val other = if (source) data.savedDestinationWorkspaceAndOrganization(destinationId) else data.savedSourceWorkspaceAndOrganization(sourceId)
          if (other != owner) throw BadRequestProblem()
          if (request.comparesSavedSourceCatalog) requireSavedSourceCatalog(data, references)
        }
        request.catalogId.ifPresent { if (!data.catalogWasFetchedByActor(it, request.actorId)) throw BadRequestProblem() }
        listOf(owner)
      }
      is ActorOperationInput.Create -> {
        val owner = data.workspaceAndOrganization(request.workspaceId)
        permissions.requireWorkspaces(caller, listOf(owner), roles, callers)
        data.requireActorDefinitionType(request.definitionId, source)
        if (!data.canUseActorDefinition(request.definitionId, owner)) throw ForbiddenProblem()
        listOf(owner)
      }
      is ActorOperationInput.Workspaces ->
        request.ids.map(data::workspaceAndOrganization).also {
          // Explicit empty requests retain the legacy instance-admin/no-auth behavior; failed lookups never enter this branch.
          if (request.ids.isEmpty()) permissions.requireGlobal(caller, roles, callers) else permissions.requireWorkspaces(caller, it, roles, callers)
        }
    }
  }
}

private fun sourceInput(args: Array<out Any?>): ActorOperationInput =
  when (val body = args.firstOrNull()) {
    is SourceIdRequestBody -> ActorOperationInput.Saved(body.sourceId)
    is SourceIdWithSecretCoordinatesRequestBody -> ActorOperationInput.Saved(body.sourceId)
    is SourceUpdate -> ActorOperationInput.Saved(body.sourceId)
    is PartialSourceUpdate -> ActorOperationInput.Saved(body.sourceId)
    is SourceDiscoverSchemaRequestBody ->
      ActorOperationInput.Saved(
        body.sourceId,
        connectionId = Optional.ofNullable(body.connectionId),
        comparesSavedSourceCatalog = true,
      )
    is SourceAutoPropagateChange ->
      ActorOperationInput.Saved(
        body.sourceId,
        Optional.of(body.workspaceId),
        catalogId = Optional.of(body.catalogId),
        hasSchemaCatalog = true,
      )
    is SourceCreate -> ActorOperationInput.Create(body.workspaceId, body.sourceDefinitionId)
    is ActorListCursorPaginatedRequestBody -> ActorOperationInput.Workspaces(listOf(body.workspaceId))
    is ListResourcesForWorkspacesRequestBody -> ActorOperationInput.Workspaces(copyIds(body.workspaceIds))
    is UUID -> ActorOperationInput.Saved(body)
    else -> throw BadRequestProblem()
  }

private fun destinationInput(args: Array<out Any?>): ActorOperationInput =
  when (val body = args.firstOrNull()) {
    is DestinationIdRequestBody -> ActorOperationInput.Saved(body.destinationId)
    is DestinationUpdate -> ActorOperationInput.Saved(body.destinationId)
    is PartialDestinationUpdate -> ActorOperationInput.Saved(body.destinationId ?: throw BadRequestProblem())
    is DestinationDiscoverSchemaRequestBody -> ActorOperationInput.Saved(body.destinationId)
    is DestinationDiscoverSchemaWriteRequestBody -> ActorOperationInput.Saved(body.destinationId ?: throw BadRequestProblem())
    is DestinationCreate ->
      ActorOperationInput.Create(
        body.workspaceId,
        body.destinationDefinitionId,
      )
    is ActorListCursorPaginatedRequestBody -> ActorOperationInput.Workspaces(listOf(body.workspaceId))
    is ListResourcesForWorkspacesRequestBody -> ActorOperationInput.Workspaces(copyIds(body.workspaceIds))
    is UUID -> ActorOperationInput.Saved(body)
    else -> throw BadRequestProblem()
  }

internal fun copyIds(ids: List<UUID?>?): List<UUID> = ids.orEmpty().map { it ?: throw BadRequestProblem() }.toList()

@Singleton
class SourceReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, true, workspaceReadRoles) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = sourceInput(args)
}

@Singleton
class SourceReadOrWorkerRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(
    data,
    permissions,
    true,
    workspaceReadRoles + AuthRoleConstants.DATAPLANE,
  ) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = sourceInput(args)
}

@Singleton
class SourceWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, true, sourceWriteRoles) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = sourceInput(args)
}

@Singleton
class SourceWriteOrWorkerRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(
    data,
    permissions,
    true,
    sourceWriteRoles + AuthRoleConstants.DATAPLANE,
  ) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = sourceInput(args)
}

@Singleton
class DestinationReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, false, workspaceReadRoles) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = destinationInput(args)
}

@Singleton
class DestinationReadOrWorkerRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(
    data,
    permissions,
    false,
    workspaceReadRoles + AuthRoleConstants.DATAPLANE,
  ) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = destinationInput(args)
}

@Singleton
class DestinationWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, false, destinationWriteRoles) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = destinationInput(args)
}

@Singleton
class DestinationCatalogWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(
    data,
    permissions,
    false,
    setOf(AuthRoleConstants.DATAPLANE),
    // Catalog results are written by the connector sidecar, with the same callers as source results.
    setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
  ) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = destinationInput(args)
}

@Singleton
class FusionSourceReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, true, setOf(AuthRoleConstants.WORKSPACE_READER), OperationCaller.entries.toSet()) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput =
    ActorOperationInput.Saved(args.firstOrNull() as? UUID ?: throw BadRequestProblem())
}

@Singleton
class FusionDestinationReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, false, setOf(AuthRoleConstants.WORKSPACE_READER), OperationCaller.entries.toSet()) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput =
    ActorOperationInput.Saved(args.firstOrNull() as? UUID ?: throw BadRequestProblem())
}

@Singleton
class FusionSourceWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, true, sourceWriteRoles, OperationCaller.entries.toSet()) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput =
    ActorOperationInput.Saved(args.firstOrNull() as? UUID ?: throw BadRequestProblem())
}

@Singleton
class FusionDestinationWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorOperationRule(data, permissions, false, destinationWriteRoles, OperationCaller.entries.toSet()) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput =
    ActorOperationInput.Saved(args.firstOrNull() as? UUID ?: throw BadRequestProblem())
}

abstract class ActorConnectionsRule(
  private val data: AuthorizationDataService,
  permissions: OperationPermissions,
  private val source: Boolean,
  roles: Set<String>,
  private val schemaChange: Boolean = false,
) : ActorOperationRule(data, permissions, source, roles) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    super.authorize(caller, input)
    val request = input as ActorOperationInput.Saved
    val id = request.actorId
    val owner = if (source) data.savedSourceWorkspaceAndOrganization(id) else data.savedDestinationWorkspaceAndOrganization(id)
    for (connectionId in data.actorConnectionIds(id, owner.workspaceId, source)) {
      val references = data.savedConnectionReferences(connectionId)
      if (connectionOwner(data, references) != owner) throw BadRequestProblem()
      if (schemaChange && request.hasSchemaCatalog) {
        requireSavedSourceCatalog(data, references)
        requireSavedDestinationCatalog(data, connectionId, owner.workspaceId, references)
      }
    }
  }
}

@Singleton
class SourceDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorConnectionsRule(data, permissions, true, sourceWriteRoles) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = sourceInput(args)
}

@Singleton
class DestinationDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorConnectionsRule(data, permissions, false, destinationWriteRoles) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = destinationInput(args)
}

@Singleton
class SourceSchemaChangeRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : ActorConnectionsRule(data, permissions, true, sourceWriteRoles, schemaChange = true) {
  override fun readInput(args: Array<out Any?>): ActorOperationInput = sourceInput(args)
}
