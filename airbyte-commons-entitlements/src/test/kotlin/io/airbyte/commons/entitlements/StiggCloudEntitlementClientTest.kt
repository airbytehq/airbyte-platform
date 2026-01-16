/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
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
  fun `addOrganization calls provisionCustomer`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    client.addOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles duplicate customer error gracefully`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException(
        "The response has errors: [Error(message = Duplicated entity not allowed, locations = null, path=null, extensions = {isValidationError=true, identifier=c00a3200-38fa-405e-9f5a-69afd6b96d27, entityName=Customer, code=DuplicatedEntityNotAllowed, traceId=b72176ae-d14d-452a-ae4b-8abdc4563a03}, nonStandardFields = null)]",
      )

    // Should not throw an exception, should handle gracefully
    client.addOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization re-throws non-duplicate ApolloException`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("Some other GraphQL error")

    // Should re-throw the exception since it's not a duplicate error
    assertThrows<ApolloException> {
      client.addOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles duplicate error with DuplicatedEntityNotAllowed code`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("GraphQL error with code: DuplicatedEntityNotAllowed")

    // Should handle gracefully
    client.addOrganization(org1, EntitlementPlan.STANDARD)

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization re-throws duplicate error with different case`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("Error: duplicated entity NOT ALLOWED for customer")

    // Should re-throw since the match is case-sensitive
    assertThrows<ApolloException> {
      client.addOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles null message in ApolloException`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException(null as String?)

    // Should re-throw since message is null
    assertThrows<ApolloException> {
      client.addOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `addOrganization handles empty message in ApolloException`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    every { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) } throws
      ApolloException("")

    // Should re-throw since message is empty
    assertThrows<ApolloException> {
      client.addOrganization(org1, EntitlementPlan.STANDARD)
    }

    verify { stigg.provisionCustomer(org1, EntitlementPlan.STANDARD) }
  }

  @Test
  fun `updateOrganization calls updateCustomerPlan`() {
    val stigg = mockk<StiggWrapper>(relaxed = true)
    val client = StiggCloudEntitlementClient(stigg, orgService)

    client.updateOrganization(org1, EntitlementPlan.PRO)

    verify { stigg.updateCustomerPlan(org1, EntitlementPlan.PRO) }
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
