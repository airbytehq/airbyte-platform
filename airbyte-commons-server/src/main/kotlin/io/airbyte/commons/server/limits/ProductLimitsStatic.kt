package io.airbyte.commons.server.limits

import java.util.UUID

class ProductLimitsStatic(
  val workspaceLimits: ProductLimitsProvider.WorkspaceLimits,
  val organizationLimits: ProductLimitsProvider.OrganizationLimits,
) : ProductLimitsProvider {
  override fun getLimitForWorkspace(workspaceId: UUID): ProductLimitsProvider.WorkspaceLimits {
    return workspaceLimits
  }

  override fun getLimitForOrganization(organizationId: UUID): ProductLimitsProvider.OrganizationLimits {
    return organizationLimits
  }
}
