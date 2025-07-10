/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementPlan
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PlatformLlmSyncJobFailureExplanation
import io.airbyte.config.Organization
import io.airbyte.data.services.OrganizationService
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.stigg.api.operations.GetEntitlementQuery
import io.stigg.api.operations.GetEntitlementsQuery
import io.stigg.api.operations.fragment.EntitlementFragment
import io.stigg.api.operations.fragment.FeatureFragment
import io.stigg.api.operations.type.FeatureType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class EntitlementClientTest {
  @Nested
  inner class DefaultEntitlementClientTest {
    private val client = DefaultEntitlementClient()

    @Test
    fun `checkEntitlement returns false`() {
      val organizationId = UUID.randomUUID()
      val entitlement = PlatformLlmSyncJobFailureExplanation
      val result = client.checkEntitlement(organizationId, entitlement)
      assertEquals(entitlement.featureId, result.featureId)
      assertEquals(false, result.isEntitled)
      assertEquals("DefaultEntitlementClient grants no entitlements", result.reason)
    }

    @Test
    fun `getEntitlements returns empty list`() {
      val result = client.getEntitlements(UUID.randomUUID())
      assertEquals(emptyList<EntitlementResult>(), result)
    }

    @Test
    fun `addOrganizationToPlan does nothing`() {
      // should not throw
      client.addOrganizationToPlan(UUID.randomUUID(), null)
    }
  }

  @Nested
  inner class StiggEntitlementClientTest {
    private val stiggMock = mockk<io.stigg.api.client.Stigg>()
    private val organizationService = mockk<OrganizationService>()

    private val client = StiggEntitlementClient(stiggMock, organizationService)

    @Test
    fun `checkEntitlement returns expected result`() {
      val organizationId = UUID.randomUUID()
      val entitlement = PlatformLlmSyncJobFailureExplanation

      every { stiggMock.query(any<GetEntitlementQuery>()) } returns
        GetEntitlementQuery.Data(
          GetEntitlementQuery.Entitlement(
            "",
            getMockEntitlementFragment(organizationId, true, entitlement.featureId),
          ),
        )

      val result = client.checkEntitlement(organizationId, entitlement)

      assertEquals(entitlement.featureId, result.featureId)
      assertEquals(true, result.isEntitled)
      assertEquals(null, result.reason)
    }

    @Test
    fun `getEntitlements maps entitlements correctly`() {
      val organizationId = UUID.randomUUID()
      val entitlement = PlatformLlmSyncJobFailureExplanation

      every { stiggMock.query(any<GetEntitlementsQuery>()) } returns
        GetEntitlementsQuery.Data(
          mutableListOf(
            GetEntitlementsQuery.Entitlement(
              "",
              getMockEntitlementFragment(organizationId, true, entitlement.featureId),
            ),
          ),
        )

      val result = client.getEntitlements(organizationId)

      assertEquals(1, result.size)
      assertEquals(entitlement.featureId, result[0].featureId)
      assertEquals(true, result[0].isEntitled)
      assertEquals(null, result[0].reason)
    }

    @Test
    fun `addOrganizationToPlan sends mutation with plan`() {
      val organizationId = UUID.randomUUID()
      val plan = EntitlementPlan.TEAMS

      val mutationSlot = slot<io.stigg.api.operations.ProvisionCustomerMutation>()
      every { stiggMock.mutation(capture(mutationSlot)) } returns mockk()
      every { organizationService.getOrganization(organizationId) } returns Optional.of(getMockOrganization(organizationId))
      client.addOrganizationToPlan(organizationId, plan)

      val input = mutationSlot.captured.input
      assertEquals(organizationId.toString(), input.customerId.getOrNull())
      assertEquals(plan.id, input.subscriptionParams.getOrNull()?.planId)
    }

    @Test
    fun `addOrganizationToPlan sends mutation with null plan`() {
      val organizationId = UUID.randomUUID()

      val mutationSlot = slot<io.stigg.api.operations.ProvisionCustomerMutation>()
      every { stiggMock.mutation(capture(mutationSlot)) } returns mockk()
      every { organizationService.getOrganization(organizationId) } returns Optional.of(getMockOrganization(organizationId))

      client.addOrganizationToPlan(organizationId, null)

      val input = mutationSlot.captured.input
      assertEquals(organizationId.toString(), input.customerId.getOrNull())
      assertEquals(null, input.subscriptionParams.getOrNull())
    }

    @Test
    fun `addOrganizationToPlan with no org throws`() {
      val organizationId = UUID.randomUUID()
      every { organizationService.getOrganization(organizationId) } returns Optional.empty()

      assertThrows<IllegalStateException> { client.addOrganizationToPlan(organizationId, null) }
    }
  }

  fun getMockEntitlementFragment(
    organizationId: UUID,
    isGranted: Boolean,
    featureId: String,
  ): EntitlementFragment =
    EntitlementFragment(
      "",
      isGranted,
      null,
      organizationId.toString(),
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      EntitlementFragment.Feature(
        "",
        FeatureFragment("", FeatureType.BOOLEAN, null, null, null, null, null, featureId, null),
      ),
    )

  fun getMockOrganization(organizationId: UUID): Organization = Organization().withOrganizationId(organizationId)
}
