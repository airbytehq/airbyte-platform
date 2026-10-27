/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.DiffCatalogsRequest
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class CatalogDiffInput(
  val currentCatalogId: UUID,
  val newCatalogId: UUID,
  val connectionId: Optional<UUID>,
  val persistChanges: Boolean,
) : OperationInput

@Singleton
class CatalogDiffRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val currentUser: CurrentUserService,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): CatalogDiffInput {
    val body = args.singleOrNull() as? DiffCatalogsRequest ?: throw BadRequestProblem()
    if (body.persistChanges == true && body.connectionId == null) {
      throw BadRequestProblem(ProblemMessageData().message("connection_id is required when persist_changes is true"))
    }
    return CatalogDiffInput(
      body.currentCatalogId,
      body.newCatalogId,
      Optional.ofNullable(body.connectionId),
      body.persistChanges == true,
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as CatalogDiffInput
    val roles = if (request.persistChanges) connectionWriteRoles else workspaceReadRoles
    if (request.connectionId.isPresent) {
      val connectionId = request.connectionId.get()
      val owner = connectionOwner(data, connectionId)
      permissions.requireWorkspaces(permissions.currentUser(caller, currentUser), listOf(owner), roles, setOf(OperationCaller.USER))
      val sourceId = data.savedConnectionActorIds(connectionId).first
      if (data.catalogWasFetchedByAnotherActor(request.currentCatalogId, sourceId) ||
        data.catalogWasFetchedByAnotherActor(request.newCatalogId, sourceId)
      ) {
        throw ForbiddenProblem()
      }
      return
    }

    val currentOwners = data.catalogSourceWorkspaceAndOrganizations(request.currentCatalogId).toSet()
    val newOwners = data.catalogSourceWorkspaceAndOrganizations(request.newCatalogId).toSet()
    val commonOwners = currentOwners.intersect(newOwners)
    if (commonOwners.isEmpty()) throw ForbiddenProblem()
    permissions.requireAnyWorkspace(permissions.currentUser(caller, currentUser), commonOwners.toList(), roles, setOf(OperationCaller.USER))
  }
}
