/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.entitlements.models.ScimEntitlement
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ScimProvisioningPilot
import io.airbyte.featureflag.TestClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class ScimAccessGateTest {
  private val entitlementService = mockk<EntitlementService>()
  private val featureFlagClient = mockk<TestClient>()
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val gate = ScimAccessGate(entitlementService, featureFlagClient)

  @Test
  fun `allows access when both entitlements and the pilot flag are enabled without requiring SSO`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(organizationId, GroupsEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns true

    assertTrue(gate.isAllowed(organizationId))

    verify(exactly = 0) { entitlementService.checkEntitlement(organizationId, SsoEntitlement) }
  }

  @Test
  fun `denies access and short circuits when the SCIM entitlement is absent`() {
    stubEntitlement(organizationId, ScimEntitlement, false)

    assertFalse(gate.isAllowed(organizationId))

    verify(exactly = 0) { entitlementService.checkEntitlement(organizationId, GroupsEntitlement) }
    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
  }

  @Test
  fun `denies access and short circuits when the groups entitlement is absent`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(organizationId, GroupsEntitlement, false)

    assertFalse(gate.isAllowed(organizationId))

    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
  }

  @Test
  fun `denies access when the pilot flag is disabled`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(organizationId, GroupsEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns false

    assertFalse(gate.isAllowed(organizationId))
  }

  @Test
  fun `re-evaluates gates on every request`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(organizationId, GroupsEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returnsMany listOf(true, false)

    assertTrue(gate.isAllowed(organizationId))
    assertFalse(gate.isAllowed(organizationId))

    verify(exactly = 2) {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    }
  }

  @Test
  fun `evaluates the pilot flag for the requested organization`() {
    val otherOrganizationId = OrganizationId(UUID.randomUUID())
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(organizationId, GroupsEntitlement, true)
    stubEntitlement(otherOrganizationId, ScimEntitlement, true)
    stubEntitlement(otherOrganizationId, GroupsEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns true
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(otherOrganizationId.value))
    } returns false

    assertTrue(gate.isAllowed(organizationId))
    assertFalse(gate.isAllowed(otherOrganizationId))
  }

  @Test
  fun `defaults to denied when the pilot flag is unconfigured`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(organizationId, GroupsEntitlement, true)
    val gateWithUnconfiguredFlag = ScimAccessGate(entitlementService, TestClient(emptyMap()))

    assertFalse(gateWithUnconfiguredFlag.isAllowed(organizationId))
  }

  private fun stubEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
    isEntitled: Boolean,
  ) {
    every { entitlementService.checkEntitlement(organizationId, entitlement) } returns
      EntitlementResult(featureId = entitlement.featureId, isEntitled = isEntitled)
  }
}
