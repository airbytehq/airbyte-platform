/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
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
import kotlin.system.measureTimeMillis

class ScimAccessGateTest {
  private val entitlementService = mockk<EntitlementService>()
  private val featureFlagClient = mockk<TestClient>()
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val gate = ScimAccessGate(entitlementService, featureFlagClient)

  @Test
  fun `allows access when the SCIM entitlement and the pilot flag are enabled without requiring SSO`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
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

    verify(exactly = 0) { featureFlagClient.boolVariation(any(), any()) }
  }

  @Test
  fun `denies access when the pilot flag is disabled`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns false

    assertFalse(gate.isAllowed(organizationId))
  }

  @Test
  fun `re-evaluates gates on every request`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
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
    stubEntitlement(otherOrganizationId, ScimEntitlement, true)
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
    val gateWithUnconfiguredFlag = ScimAccessGate(entitlementService, TestClient(emptyMap()))

    assertFalse(gateWithUnconfiguredFlag.isAllowed(organizationId))
  }

  @Test
  fun `caches the organization-info result so the boot path re-checks nothing`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns true

    assertTrue(gate.isAllowedForOrganizationInfo(organizationId))
    assertTrue(gate.isAllowedForOrganizationInfo(organizationId))

    verify(exactly = 1) { entitlementService.checkEntitlement(organizationId, ScimEntitlement) }
    verify(exactly = 1) {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    }
  }

  @Test
  fun `caches the organization-info result per organization`() {
    val otherOrganizationId = OrganizationId(UUID.randomUUID())
    stubEntitlement(organizationId, ScimEntitlement, true)
    stubEntitlement(otherOrganizationId, ScimEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns true
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(otherOrganizationId.value))
    } returns false

    assertTrue(gate.isAllowedForOrganizationInfo(organizationId))
    assertFalse(gate.isAllowedForOrganizationInfo(otherOrganizationId))
  }

  @Test
  fun `reports the advisory flag as disabled when the gate outlives its budget`() {
    // Stands in for a hung Stigg sidecar, which blocks for the full 30s STIGG_TIMEOUT_MS. Every
    // other input says "allowed", so without the budget this call returns true after ~30s and the
    // assertions below fail on both the value and the elapsed time.
    every { entitlementService.checkEntitlement(organizationId, ScimEntitlement) } answers {
      Thread.sleep(30_000)
      EntitlementResult(featureId = ScimEntitlement.featureId, isEntitled = true)
    }
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returns true

    val elapsed =
      measureTimeMillis {
        assertFalse(gate.isAllowedForOrganizationInfo(organizationId))
      }

    assertTrue(elapsed < 15_000, "the advisory read should abandon the lookup, but took ${elapsed}ms")
  }

  @Test
  fun `leaves isAllowed uncached so authentication sees a revoked gate immediately`() {
    stubEntitlement(organizationId, ScimEntitlement, true)
    every {
      featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
    } returnsMany listOf(true, false)

    assertTrue(gate.isAllowedForOrganizationInfo(organizationId))
    // The gate flips, and the uncached path reports it on the very next call.
    assertFalse(gate.isAllowed(organizationId))
    // The advisory flag stays cached until its TTL expires.
    assertTrue(gate.isAllowedForOrganizationInfo(organizationId))
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
