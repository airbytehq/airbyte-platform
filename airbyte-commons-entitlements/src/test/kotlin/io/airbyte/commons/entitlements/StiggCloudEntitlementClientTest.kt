/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceUnableToAddOrganizationProblem
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.config.Organization
import io.airbyte.data.services.OrganizationService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import io.stigg.sidecar.sdk.offline.Entitlement
import io.stigg.sidecar.sdk.offline.EntitlementType
import io.stigg.sidecar.sdk.offline.OfflineEntitlements
import io.stigg.sidecar.sdk.offline.OfflineStiggConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

internal class StiggCloudEntitlementClientTest {
  val org1 = OrganizationId(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  val org2 = OrganizationId(UUID.fromString("22222222-2222-2222-2222-222222222222"))
  val org3 = OrganizationId(UUID.fromString("33333333-3333-3333-3333-333333333333"))

  val orgService = mockk<OrganizationService>()

  @Test
  fun checkEntitlement() {
    val stigg =
      buildOfflineClient(
        org1 to "feature-a",
        org1 to "feature-b",
        org2 to "feature-c",
      )
    val client = StiggCloudEntitlementClient(stigg, orgService)
    client.checkEntitlement(org1, FeatureEntitlement("feature-a")).assertEntitled()
    client.checkEntitlement(org1, FeatureEntitlement("feature-b")).assertEntitled()
    client.checkEntitlement(org1, FeatureEntitlement("feature-c")).assertNotEntitled()
    client.checkEntitlement(org2, FeatureEntitlement("feature-a")).assertNotEntitled()
    client.checkEntitlement(org2, FeatureEntitlement("feature-b")).assertNotEntitled()
    client.checkEntitlement(org2, FeatureEntitlement("feature-c")).assertEntitled()
  }

  @Test
  fun getEntitlements() {
    val stigg =
      buildOfflineClient(
        org1 to "feature-a",
        org1 to "feature-b",
        org2 to "feature-c",
      )
    val client = StiggCloudEntitlementClient(stigg, orgService)

    assertEquals(
      listOf<EntitlementResult>(
        EntitlementResult("feature-a", true),
        EntitlementResult("feature-b", true),
      ),
      client.getEntitlements(org1),
    )

    assertEquals(
      listOf<EntitlementResult>(
        EntitlementResult("feature-c", true),
      ),
      client.getEntitlements(org2),
    )

    assertEquals(listOf<EntitlementResult>(), client.getEntitlements(org3))
  }

  @Test
  fun `addOrganization calls validatePlanChange and provisionCustomer`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)
    val plan = EntitlementPlan.STANDARD

    every { stigg.getPlans(org1) } returns emptyList()

    client.addOrganization(org1, plan)

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, plan) }
  }

  @Test
  fun addOrganization() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()

    client.addOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.PRO) }
  }

  @Test
  fun `upgrade from standard to pro`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlan.STANDARD)

    client.addOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.PRO) }
  }

  @Test
  fun `adding back to same plan is a no-op`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlan.STANDARD)

    client.addOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `cannot downgrade from pro to standard`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlan.PRO)

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrganization(org1, EntitlementPlan.STANDARD)
      }

    val data = exception.problem.getData() as ProblemEntitlementServiceData
    assertEquals(org1.value, data.organizationId)
    assertEquals(EntitlementPlan.STANDARD.toString(), data.planId)
    assertEquals(
      "Cannot automatically downgrade from PRO (value: 2) to STANDARD (value: 0)",
      data.errorMessage,
    )
  }

  @Test
  fun `cannot downgrade from pro trial to standard`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlan.PRO_TRIAL)

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrganization(org1, EntitlementPlan.STANDARD)
      }

    val data = exception.problem.getData() as ProblemEntitlementServiceData
    assertEquals(org1.value, data.organizationId)
    assertEquals(EntitlementPlan.STANDARD.toString(), data.planId)
    assertEquals(
      "Cannot automatically downgrade from PRO_TRIAL (value: 2) to STANDARD (value: 0)",
      data.errorMessage,
    )
  }

  @Test
  fun `plan validation handles multiple plans when checking for downgrades`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlan.CORE, EntitlementPlan.PRO)

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrganization(org1, EntitlementPlan.STANDARD)
      }

    val data = exception.problem.getData() as ProblemEntitlementServiceData
    assertEquals(org1.value, data.organizationId)
    assertEquals(EntitlementPlan.STANDARD.toString(), data.planId)
    // Should mention the highest value plan (PRO)
    assertEquals(
      "Cannot automatically downgrade from PRO (value: 2) to STANDARD (value: 0)",
      data.errorMessage,
    )
  }

  @Test
  fun `addOrganization handles stigg wrapper exceptions gracefully`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } throws RuntimeException("Stigg service unavailable")

    assertThrows<RuntimeException> {
      client.addOrganization(org1, EntitlementPlan.STANDARD)
    }
  }

  @Test
  fun `addOrganization works with organization not found in stigg`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    // Simulate organization not being found in Stigg (empty plans)
    every { stigg.getPlans(org1) } returns emptyList()

    client.addOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }
}

private fun EntitlementResult.assertEntitled() {
  assertEquals(true, this.isEntitled)
}

private fun EntitlementResult.assertNotEntitled() {
  assertEquals(false, this.isEntitled)
}

private fun buildOfflineClient(vararg entitlements: Pair<OrganizationId, String>) =
  StiggWrapper(
    Stigg.init(
      OfflineStiggConfig
        .builder()
        .entitlements(
          OfflineEntitlements
            .builder()
            .customers(
              entitlements
                .groupBy({ it.first }, { it.second })
                .entries
                .associate { entry ->
                  entry.key.value.toString() to
                    CustomerEntitlements
                      .builder()
                      .entitlements(
                        entry.value.associate {
                          it to Entitlement.builder().type(EntitlementType.BOOLEAN).build()
                        },
                      ).build()
                },
            ).build(),
        ).build(),
    ),
  )
