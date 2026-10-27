/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.ServiceAccountNotFound
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID
import io.airbyte.data.repositories.entities.DataplaneGroup as SavedRegion

class DataplaneAuthorizationDataServiceTest {
  private val organization = UUID(0, 2)
  private val group = UUID(0, 3)
  private val dataplane = UUID(0, 4)
  private val client = UUID(0, 9)
  private val dataplanes = mockk<DataplaneService>()
  private val groups = mockk<DataplaneGroupService>()
  private val regions = mockk<DataplaneGroupRepository>()
  private val data = mockk<AuthorizationDataService>()
  private val entitlements = mockk<EntitlementService>()
  private val service = DataplaneAuthorizationDataService(dataplanes, groups, regions, data, entitlements)

  @Test
  fun `machine projection verifies the saved client assignment and never fetches service account credentials`() {
    val saved = Dataplane().withId(dataplane).withDataplaneGroupId(group).withServiceAccountId(client)
    every { dataplanes.getDataplaneByServiceAccountId(client.toString()) } returns saved
    every { data.dataplaneGroupOrganization(group) } returns organization
    assertEquals(organization, service.clientOrganization(client.toString()))
    verify(exactly = 0) { dataplanes.createDataplaneAndServiceAccount(any(), any()) }
    saved.serviceAccountId = UUID(0, 10)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.clientOrganization(client.toString()) }
    saved.serviceAccountId = client
    saved.dataplaneGroupId = null
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.clientOrganization(client.toString()) }
    every { dataplanes.getDataplaneByServiceAccountId(client.toString()) } returns null
    assertThrows(ServiceAccountNotFound::class.java) { service.clientOrganization(client.toString()) }
    assertThrows(IllegalArgumentException::class.java) { service.clientOrganization("malformed") }
    val failure = IOException("assignment lookup unavailable")
    every { dataplanes.getDataplaneByServiceAccountId(client.toString()) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.clientOrganization(client.toString()) })
  }

  @Test
  fun `source and removal permit deleted regions while destinations reject deleted or missing regions`() {
    val region = SavedRegion(group, organization, "fixture", true, tombstone = true)
    every { regions.findById(group) } returns Optional.of(region)
    every { data.organization(organization) } returns organization
    service.requireRegion(group, false)
    assertThrows(BadRequestProblem::class.java) { service.requireRegion(group, true) }
    region.tombstone = false
    service.requireRegion(group, true)
    region.id = UUID(0, 99)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireRegion(group, false) }
    every { regions.findById(group) } returns Optional.empty()
    assertThrows(BadRequestProblem::class.java) { service.requireRegion(group, false) }
    val failure = IOException("region metadata unavailable")
    every { regions.findById(group) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.requireRegion(group, true) })
  }

  @Test
  fun `destination eligibility uses actual shared private link and self managed entitlement owners`() {
    every { entitlements.checkEntitlement(OrganizationId(organization), PrivateLinkEntitlement) } returns
      EntitlementResult(PrivateLinkEntitlement.featureId, false)
    every { entitlements.checkEntitlement(OrganizationId(organization), SelfManagedRegionsEntitlement) } returns
      EntitlementResult(SelfManagedRegionsEntitlement.featureId, false)
    every { groups.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), true) } returns
      listOf(DataplaneGroup().withId(group).withOrganizationId(organization))
    assertThrows(BadRequestProblem::class.java) { service.requireUsableRegion(organization, group) }
    every { groups.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), true) } returns
      listOf(DataplaneGroup().withId(group).withOrganizationId(DEFAULT_ORGANIZATION_ID).withTombstone(true))
    service.requireUsableRegion(organization, group)
    every { entitlements.checkEntitlement(OrganizationId(organization), PrivateLinkEntitlement) } returns
      EntitlementResult(PrivateLinkEntitlement.featureId, true)
    every { entitlements.checkEntitlement(OrganizationId(organization), SelfManagedRegionsEntitlement) } returns
      EntitlementResult(SelfManagedRegionsEntitlement.featureId, true)
    every { groups.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID, organization), true) } returns
      listOf(DataplaneGroup().withId(group).withOrganizationId(organization))
    service.requireUsableRegion(organization, group)
    verify(exactly = 0) { regions.findById(any()) }
  }

  @Test
  fun `default region is configured saved metadata without customer eligibility or tombstone inference`() {
    every { groups.getDefaultDataplaneGroup() } returns DataplaneGroup().withId(group).withOrganizationId(DEFAULT_ORGANIZATION_ID).withTombstone(true)
    every { data.organization(DEFAULT_ORGANIZATION_ID) } returns DEFAULT_ORGANIZATION_ID
    service.requireDefaultRegion()
    verify(exactly = 0) { entitlements.checkEntitlement(any(), any()) }
    every { groups.getDefaultDataplaneGroup() } returns DataplaneGroup().withId(group)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.requireDefaultRegion() }
    val failure = IllegalStateException("default not configured")
    every { groups.getDefaultDataplaneGroup() } throws failure
    assertSame(failure, assertThrows(IllegalStateException::class.java) { service.requireDefaultRegion() })
  }
}
