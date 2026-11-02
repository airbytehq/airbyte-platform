/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.publicApi.server.generated.models.WorkspaceUpdateRequest
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

abstract class PublicActorDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
  private val source: Boolean,
) : PublicResourceRule(
    data,
    permissions,
    if (source) PublicResourceKind.SOURCE else PublicResourceKind.DESTINATION,
    if (source) sourceWriteRoles + AuthRoleConstants.EMBEDDED_END_USER else destinationWriteRoles,
    if (source) setOf(OperationCaller.USER, OperationCaller.EMBEDDED) else setOf(OperationCaller.USER),
  ) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    super.authorize(if (source) caller else permissions.scopedEmbeddedIssuer(caller), input)
    val id = (input as PublicResourceInput).id
    val owner = if (source) data.savedSourceWorkspaceAndOrganization(id) else data.savedDestinationWorkspaceAndOrganization(id)
    for (connection in data.actorConnectionIds(id, owner.workspaceId, source)) {
      val saved = data.savedConnectionReferences(connection)
      if ((if (source) saved.sourceId else saved.destinationId) != id || connectionOwner(data, saved) != owner) throw BadRequestProblem()
      for (operation in saved.operationIds) if (data.operationWorkspaceAndOrganization(operation) != owner) throw BadRequestProblem()
    }
  }
}

@Singleton
class PublicSourceDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicActorDeleteRule(data, permissions, true)

@Singleton
class PublicDestinationDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicActorDeleteRule(data, permissions, false)

@Singleton
class PublicConnectionDeleteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.CONNECTION, publicWriteRoles) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    super.authorize(caller, input)
    val id = (input as PublicResourceInput).id
    val saved = data.savedConnectionReferences(id)
    val owner = connectionOwner(data, saved)
    for (operation in saved.operationIds) if (data.operationWorkspaceAndOrganization(operation) != owner) throw BadRequestProblem()
  }
}

@Singleton
class PublicConnectionPatchRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.CONNECTION, publicWriteRoles)

data class PublicWorkspaceUpdateInput(
  val id: UUID,
  val region: Optional<UUID>,
) : OperationInput

@Singleton
class PublicWorkspaceUpdateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.WORKSPACE, publicWriteRoles) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicWorkspaceUpdateInput(
      publicResourceId(args.firstOrNull()),
      Optional.ofNullable(
        (
          args.getOrNull(1) as? WorkspaceUpdateRequest
            ?: throw BadRequestProblem()
        ).regionId,
      ),
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as PublicWorkspaceUpdateInput
    super.authorize(caller, PublicResourceInput(request.id))
    val owner = data.savedWorkspaceAndOrganization(request.id, false)
    request.region.ifPresent { if (!data.canUseDataplaneGroup(it, owner.organizationId)) throw ForbiddenProblem() }
  }
}
