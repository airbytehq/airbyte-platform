/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.limits

import java.util.UUID

class ProductLimitsStatic(
  val workspaceLimits: ProductLimitsProvider.WorkspaceLimits,
  val organizationLimits: ProductLimitsProvider.OrganizationLimits,
) : ProductLimitsProvider {
  override fun getLimitForWorkspace(workspaceId: UUID): ProductLimitsProvider.WorkspaceLimits = workspaceLimits

  override fun getLimitForOrganization(organizationId: UUID): ProductLimitsProvider.OrganizationLimits = organizationLimits
}
