/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.ServiceAccountNotFound
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import jakarta.inject.Singleton
import java.util.UUID

/** Saved assignments and region metadata only; service-account credentials are never read. */
@Singleton
class DataplaneAuthorizationDataService(
  private val dataplanes: DataplaneService,
  private val groups: DataplaneGroupService,
  private val regions: DataplaneGroupRepository,
  private val data: AuthorizationDataService,
  private val entitlements: EntitlementService,
) {
  fun clientOrganization(clientId: String): UUID {
    val serviceAccountId = UUID.fromString(clientId)
    val dataplane = dataplanes.getDataplaneByServiceAccountId(clientId) ?: throw ServiceAccountNotFound(serviceAccountId)
    if (dataplane.serviceAccountId != serviceAccountId || dataplane.id == null) throw AuthorizationResourceNotFoundException()
    val groupId = dataplane.dataplaneGroupId ?: throw AuthorizationResourceNotFoundException()
    return data.dataplaneGroupOrganization(groupId)
  }

  fun requireRegion(
    regionId: UUID,
    destination: Boolean,
  ) {
    val region =
      regions.findById(regionId).orElseThrow {
        BadRequestProblem(ProblemMessageData().message("Region $regionId does not exist."))
      }
    if (region.id != regionId) throw AuthorizationResourceNotFoundException()
    data.organization(region.organizationId)
    if (destination && region.tombstone) throw BadRequestProblem(ProblemMessageData().message("Destination region $regionId has been deleted."))
  }

  fun requireUsableRegion(
    organizationId: UUID,
    regionId: UUID,
  ) {
    val owners = mutableListOf(DEFAULT_ORGANIZATION_ID)
    if (entitlements
        .checkEntitlement(
          OrganizationId(organizationId),
          PrivateLinkEntitlement,
        ).isEntitled
    ) {
      owners.add(PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)
    }
    if (entitlements.checkEntitlement(OrganizationId(organizationId), SelfManagedRegionsEntitlement).isEntitled) owners.add(organizationId)
    if (groups.listDataplaneGroups(owners, true).none { it.id == regionId && it.organizationId in owners }) {
      throw BadRequestProblem(ProblemMessageData().message("Organization $organizationId may not use region $regionId."))
    }
  }

  fun requireDefaultRegion() {
    val region = groups.getDefaultDataplaneGroup()
    if (region.id == null) throw AuthorizationResourceNotFoundException()
    data.organization(region.organizationId ?: throw AuthorizationResourceNotFoundException())
  }
}
