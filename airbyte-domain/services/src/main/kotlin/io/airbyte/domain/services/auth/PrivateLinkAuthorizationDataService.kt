/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigScopeType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.PrivateLinkService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.NetworkSecurityTokenKey
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class PrivateLinkAuthorizationMetadata(
  val id: UUID,
  val owner: WorkspaceAndOrganization,
  val group: UUID,
  val configuration: Optional<UUID>,
)

class PrivateLinkReferenceMismatchException : RuntimeException("Private link configuration must belong to the saved link and workspace.")

@Singleton
class PrivateLinkAuthorizationDataService(
  private val data: AuthorizationDataService,
  private val links: PrivateLinkService,
  private val workspaces: WorkspaceService,
  private val configs: ScopedConfigurationService,
) {
  fun saved(
    id: UUID,
    includeDeletedWorkspace: Boolean = false,
  ): PrivateLinkAuthorizationMetadata {
    val link =
      try {
        links.getPrivateLink(id)
      } catch (_: NoSuchElementException) {
        throw AuthorizationResourceNotFoundException()
      }
    if (link.id != id) throw AuthorizationResourceNotFoundException()
    return PrivateLinkAuthorizationMetadata(
      id,
      data.savedWorkspaceAndOrganization(link.workspaceId, includeDeletedWorkspace),
      link.dataplaneGroupId,
      Optional.ofNullable(link.scopedConfigurationId),
    )
  }

  fun createGroup(owner: WorkspaceAndOrganization): UUID {
    val workspace = workspaces.getStandardWorkspaceNoSecrets(owner.workspaceId, false)
    if (workspace.workspaceId != owner.workspaceId || workspace.organizationId != owner.organizationId) throw AuthorizationResourceNotFoundException()
    return requireNotNull(workspace.dataplaneGroupId) { "Workspace ${workspace.workspaceId} has no dataplane group assigned" }
  }

  fun groupUsable(
    group: UUID,
    owner: WorkspaceAndOrganization,
  ): Boolean = data.canUseDataplaneGroup(group, owner.organizationId)

  fun requireConfiguration(
    id: UUID,
    link: PrivateLinkAuthorizationMetadata,
  ) {
    val config =
      try {
        configs.getScopedConfigurationMetadata(id)
      } catch (
        _: ConfigNotFoundException,
      ) {
        throw AuthorizationResourceNotFoundException()
      }
    if (config.id != id) throw AuthorizationResourceNotFoundException()
    if (config.key != NetworkSecurityTokenKey.key || config.scopeType != ConfigScopeType.WORKSPACE ||
      config.scopeId != link.owner.workspaceId
    ) {
      throw PrivateLinkReferenceMismatchException()
    }
    if (config.originType != ConfigOriginType.PRIVATE_LINK || config.origin != link.id.toString()) throw PrivateLinkReferenceMismatchException()
    if (config.resourceType.isPresent || config.resourceId.isPresent) throw PrivateLinkReferenceMismatchException()
  }
}
