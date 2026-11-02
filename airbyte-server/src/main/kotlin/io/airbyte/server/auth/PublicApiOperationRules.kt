/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

internal fun publicResourceId(value: Any?): UUID =
  when (value) {
    is UUID -> value
    is String ->
      try {
        UUID.fromString(value)
      } catch (_: IllegalArgumentException) {
        throw BadRequestProblem()
      }
    else -> throw BadRequestProblem()
  }

/** Preserve the existing public Config API not-found payload when authorization resolves that resource first. */
internal fun <T> publicConfigResource(
  type: ConfigNotFoundType,
  id: UUID,
  lookup: () -> T,
): T =
  try {
    lookup()
  } catch (failure: AuthorizationResourceNotFoundException) {
    throw ResourceNotFoundProblem(ProblemResourceData().resourceType(type.name).resourceId(id.toString())).apply { initCause(failure) }
  }

enum class PublicResourceKind { SOURCE, DESTINATION, CONNECTION, WORKSPACE, ORGANIZATION, TAG, REGION, DATAPLANE }

data class PublicResourceInput(
  val id: UUID,
) : OperationInput

/** Resolve path IDs from saved data, with the same action as the original public method. */
abstract class PublicResourceRule(
  protected val data: AuthorizationDataService,
  protected val permissions: OperationPermissions,
  private val kind: PublicResourceKind,
  private val roles: Set<String>,
  private val callers: Set<OperationCaller> = setOf(OperationCaller.USER),
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = PublicResourceInput(publicResourceId(args.firstOrNull()))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireCaller(caller, callers)
    val id = (input as PublicResourceInput).id
    val target =
      when (kind) {
        PublicResourceKind.SOURCE -> AuthorizationTarget.Workspace(data.savedSourceWorkspaceAndOrganization(id))
        PublicResourceKind.DESTINATION -> AuthorizationTarget.Workspace(data.savedDestinationWorkspaceAndOrganization(id))
        PublicResourceKind.CONNECTION -> AuthorizationTarget.Workspace(connectionOwner(data, id))
        PublicResourceKind.WORKSPACE -> AuthorizationTarget.Workspace(data.savedWorkspaceAndOrganization(id, false))
        PublicResourceKind.TAG ->
          AuthorizationTarget.Workspace(
            publicConfigResource(ConfigNotFoundType.TAG, id) { data.tagWorkspaceAndOrganization(id) },
          )
        PublicResourceKind.ORGANIZATION -> AuthorizationTarget.Organization(data.organization(id))
        PublicResourceKind.REGION -> AuthorizationTarget.Organization(data.dataplaneGroupOrganization(id))
        PublicResourceKind.DATAPLANE -> AuthorizationTarget.Organization(data.dataplaneOrganization(id))
      }
    requireAuthorizationTarget(permissions, caller, target, roles, callers)
  }
}

private val publicSourceCallers = setOf(OperationCaller.USER, OperationCaller.EMBEDDED)
private val publicSourceReadRoles = setOf(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.EMBEDDED_END_USER)
private val publicSourceWriteRoles =
  setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.EMBEDDED_END_USER)
internal val publicWriteRoles =
  setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR)

@Singleton
class PublicSourceReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.SOURCE, publicSourceReadRoles, publicSourceCallers)

@Singleton
class PublicSourceWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.SOURCE, publicSourceWriteRoles, publicSourceCallers)

@Singleton
class PublicDestinationReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.DESTINATION, setOf(AuthRoleConstants.WORKSPACE_READER)) {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = super.authorize(permissions.scopedEmbeddedIssuer(caller), input)
}

@Singleton
class PublicDestinationWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(
    data,
    permissions,
    PublicResourceKind.DESTINATION,
    setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR),
  )

@Singleton
class PublicConnectionReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.CONNECTION, setOf(AuthRoleConstants.WORKSPACE_READER))

@Singleton
class PublicWorkspaceReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.WORKSPACE, setOf(AuthRoleConstants.WORKSPACE_READER))

@Singleton
class PublicWorkspaceWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.WORKSPACE, publicWriteRoles)

@Singleton
class PublicTagReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.TAG, setOf(AuthRoleConstants.WORKSPACE_READER))

@Singleton
class PublicTagWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.TAG, publicWriteRoles)

@Singleton
class PublicOrganizationAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.ORGANIZATION, setOf(AuthRoleConstants.ORGANIZATION_ADMIN))

@Singleton
class PublicOrganizationMemberRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.ORGANIZATION, setOf(AuthRoleConstants.ORGANIZATION_MEMBER))

@Singleton
class PublicRegionAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.REGION, setOf(AuthRoleConstants.ORGANIZATION_ADMIN))

@Singleton
class PublicDataplaneAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.DATAPLANE, setOf(AuthRoleConstants.ORGANIZATION_ADMIN))

data class PublicWorkspaceListInput(
  val ids: List<UUID>,
  val includeDeleted: Boolean,
) : OperationInput

/** Empty selectors keep the existing current-user filtered list, rather than becoming a global action. */
@Singleton
class PublicWorkspaceListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    @Suppress("UNCHECKED_CAST")
    val ids = copyIds(args.firstOrNull() as? List<UUID?>)
    return PublicWorkspaceListInput(ids, args.getOrNull(1) as? Boolean ?: false)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as PublicWorkspaceListInput
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    if (request.ids.isEmpty()) {
      permissions.requireAuthenticated(caller, callers)
      return
    }
    val owners = request.ids.map { data.savedWorkspaceAndOrganization(it, request.includeDeleted) }
    permissions.requireWorkspaces(caller, owners, setOf(AuthRoleConstants.WORKSPACE_READER), callers)
  }
}

@Singleton
class PublicCurrentUserRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = MetadataOperationInput

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
  }
}
