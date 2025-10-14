/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceUnableToAddOrganizationProblem
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
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

    client.addOrUpdateOrganization(org1, plan)

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, plan) }
  }

  @Test
  fun addOrganization() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()

    client.addOrUpdateOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.PRO) }
  }

  @Test
  fun `upgrade from standard to pro`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, "plan-airbyte-standard", "Standard"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.PRO) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `adding back to same plan is a no-op`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, "plan-airbyte-standard", "Standard"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.getPlans(org1) }
    verify(exactly = 0) { stigg.updateCustomerPlan(any(), any()) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `cannot downgrade from pro to standard`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.PRO, "plan-airbyte-pro", "Pro"))

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
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

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.PRO_TRIAL, "plan-airbyte-unified-trial", "Pro Trial"))

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
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
  fun `plan validation handles single plan when checking for downgrades`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.PRO, "plan-airbyte-pro", "Pro"))

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
      }

    val data = exception.problem.getData() as ProblemEntitlementServiceData
    assertEquals(org1.value, data.organizationId)
    assertEquals(EntitlementPlan.STANDARD.toString(), data.planId)
    // Should mention the current plan (PRO)
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
      client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
    }
  }

  @Test
  fun `addOrganization works with organization not found in stigg`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    // Simulate organization not being found in Stigg (empty plans)
    every { stigg.getPlans(org1) } returns emptyList()

    client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles duplicate customer error gracefully`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()
    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException(
        "The response has errors: [Error(message = Duplicated entity not allowed, locations = null, path=null, extensions = {isValidationError=true, identifier=c00a3200-38fa-405e-9f5a-69afd6b96d27, entityName=Customer, code=DuplicatedEntityNotAllowed, traceId=b72176ae-d14d-452a-ae4b-8abdc4563a03}, nonStandardFields = null)]",
      )

    // Should not throw an exception, should handle gracefully
    client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization re-throws non-duplicate ApolloException`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()
    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("Some other GraphQL error")

    // Should re-throw the exception since it's not a duplicate error
    assertThrows<ApolloException> {
      client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization calls updateCustomerPlan when organization already exists with plans`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.CORE, "plan-airbyte-core", "Core"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.getPlans(org1) }
    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.STANDARD) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `addOrganization calls updateCustomerPlan for plan upgrade with existing customer`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, "plan-airbyte-standard", "Standard"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.getPlans(org1) }
    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.PRO) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `addOrganization returns early when already on same plan`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, "plan-airbyte-standard", "Standard"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.getPlans(org1) }
    // Should return early and not call updateCustomerPlan or provisionCustomer
    verify(exactly = 0) { stigg.updateCustomerPlan(any(), any()) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `addOrganization re-throws ApolloException from updateCustomerPlan`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.CORE, "plan-airbyte-core", "Core"))
    every { stigg.updateCustomerPlan(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("Some GraphQL error from updateCustomerPlan")

    // Should re-throw since updateCustomerPlan doesn't handle ApolloException
    assertThrows<ApolloException> {
      client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.getPlans(org1) }
    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization returns early when multiple plans found`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    // Test with multiple plans - this should now return early with error log
    every { stigg.getPlans(org1) } returns
      listOf(
        EntitlementPlanResponse(EntitlementPlan.PRO, "plan-airbyte-pro", "Pro"),
        EntitlementPlanResponse(EntitlementPlan.PRO_TRIAL, "plan-airbyte-unified-trial", "Pro Trial"),
      )

    // Should return early without throwing an exception or calling any other methods
    client.addOrUpdateOrganization(org1, EntitlementPlan.CORE)

    verify { stigg.getPlans(org1) }
    // Should not call updateCustomerPlan or provisionCustomer due to early return
    verify(exactly = 0) { stigg.updateCustomerPlan(any(), any()) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `addOrganization with existing plans - allows lateral moves between same value plans`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    // PRO and PRO_TRIAL both have value 2, so moving between them should be allowed
    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.PRO, "plan-airbyte-pro", "Pro"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.PRO_TRIAL)

    verify { stigg.getPlans(org1) }
    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.PRO_TRIAL) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `addOrganization with existing plan - prevents downgrade from high value plan`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    // Single high-value plan - should prevent downgrade
    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.PRO, "plan-airbyte-pro", "Pro"))

    val exception =
      assertThrows<EntitlementServiceUnableToAddOrganizationProblem> {
        client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
      }

    val data = exception.problem.getData() as ProblemEntitlementServiceData
    assertEquals(org1.value, data.organizationId)
    assertEquals(EntitlementPlan.STANDARD.toString(), data.planId)
    assert(data.errorMessage.contains("Cannot automatically downgrade from PRO"))
    assert(data.errorMessage.contains("STANDARD (value: 0)"))
  }

  @Test
  fun `addOrganization with existing plan - allows upgrade from low to high value plan`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    // Single low-value plan - should allow upgrade to higher value
    every { stigg.getPlans(org1) } returns listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, "plan-airbyte-standard", "Standard"))

    client.addOrUpdateOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.getPlans(org1) }
    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.PRO) }
    verify(exactly = 0) { stigg.provisionCustomer(any(), any()) }
  }

  @Test
  fun `addOrganization handles duplicate error with DuplicatedEntityNotAllowed code`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()
    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("GraphQL error with code: DuplicatedEntityNotAllowed")

    // Should handle gracefully
    client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization re-throws duplicate error with different case`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()
    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("Error: duplicated entity NOT ALLOWED for customer")

    // Should re-throw since the match is case-sensitive
    assertThrows<ApolloException> {
      client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles null message in ApolloException`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()
    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException(null as String?)

    // Should re-throw since message is null
    assertThrows<ApolloException> {
      client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.getPlans(org1) }
    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles empty message in ApolloException`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.getPlans(org1) } returns emptyList()
    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("")

    // Should re-throw since message is empty
    assertThrows<ApolloException> {
      client.addOrUpdateOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.getPlans(org1) }
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
