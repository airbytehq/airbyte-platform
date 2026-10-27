/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.GetEntitlementsByOrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationIsEntitledRequestBody
import io.airbyte.api.server.generated.models.UpdateOrganizationEntitlementPlanRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class EntitlementsApiControllerTest {
  private lateinit var entitlementService: EntitlementService
  private lateinit var entitlementsApiController: EntitlementsApiController

  private val organizationId = UUID.randomUUID()
  private val featureId = "feature-sso"

  @BeforeEach
  fun setup() {
    entitlementService = mockk()
    entitlementsApiController = EntitlementsApiController(entitlementService)
  }

  @Test
  fun testCheckEntitlement() {
    val requestBody = OrganizationIsEntitledRequestBody(organizationId = organizationId, featureId = featureId)
    val entitlementResult = EntitlementResult(featureId, true, null)

    every { entitlementService.checkEntitlement(OrganizationId(organizationId), any()) } returns entitlementResult

    val result = entitlementsApiController.checkEntitlement(requestBody)

    assertNotNull(result)
    assertEquals(featureId, result.featureId)
    assertEquals(true, result.isEntitled)
    verify(exactly = 1) { entitlementService.checkEntitlement(OrganizationId(organizationId), any()) }
  }

  @Test
  fun testCheckEntitlementThrowsOnUnknownEntitlement() {
    val requestBody = OrganizationIsEntitledRequestBody(organizationId = organizationId, featureId = "not-a-featuer")
    val entitlementResult = EntitlementResult(featureId, true, null)

    every { entitlementService.checkEntitlement(OrganizationId(organizationId), any()) } returns entitlementResult

    assertThrows<IllegalArgumentException> { entitlementsApiController.checkEntitlement(requestBody) }
  }

  @Test
  fun testGetEntitlementsSucceedsWhenValidationPasses() {
    val requestBody = GetEntitlementsByOrganizationIdRequestBody(organizationId = organizationId)
    val entitlementResults =
      listOf(
        EntitlementResult("feature1", true, null),
        EntitlementResult("feature2", false, "Insufficient plan"),
        EntitlementResult(featureId = "feature-maximum-workspaces", isEntitled = true, value = 3L, isUnlimited = false),
      )

    every { entitlementService.getEntitlements(OrganizationId(organizationId)) } returns entitlementResults

    val result = entitlementsApiController.getEntitlements(requestBody)

    assertNotNull(result)
    assertEquals(3, result.entitlements.size)

    val firstEntitlement = result.entitlements[0]
    assertEquals("feature1", firstEntitlement.featureId)
    assertEquals(true, firstEntitlement.isEntitled)
    assertEquals(null, firstEntitlement.accessDeniedReason)

    val secondEntitlement = result.entitlements[1]
    assertEquals("feature2", secondEntitlement.featureId)
    assertEquals(false, secondEntitlement.isEntitled)
    assertEquals("Insufficient plan", secondEntitlement.accessDeniedReason)

    val numericEntitlement = result.entitlements[2]
    assertEquals("feature-maximum-workspaces", numericEntitlement.featureId)
    assertEquals(true, numericEntitlement.isEntitled)
    assertEquals(3L, numericEntitlement.value)
    assertEquals(false, numericEntitlement.isUnlimited)

    verify(exactly = 1) { entitlementService.getEntitlements(OrganizationId(organizationId)) }
  }

  @Test
  fun testGetEntitlementsPropagatesServiceDenial() {
    val requestBody = GetEntitlementsByOrganizationIdRequestBody(organizationId = organizationId)
    every { entitlementService.getEntitlements(OrganizationId(organizationId)) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> { entitlementsApiController.getEntitlements(requestBody) }
    verify(exactly = 1) { entitlementService.getEntitlements(OrganizationId(organizationId)) }
  }

  @Test
  fun testGetEntitlementsWithEmptyResults() {
    val requestBody = GetEntitlementsByOrganizationIdRequestBody(organizationId = organizationId)

    every { entitlementService.getEntitlements(OrganizationId(organizationId)) } returns emptyList()

    val result = entitlementsApiController.getEntitlements(requestBody)

    assertNotNull(result)
    assertEquals(0, result.entitlements.size)

    verify(exactly = 1) { entitlementService.getEntitlements(OrganizationId(organizationId)) }
  }

  @Test
  fun testListAllEntitlementPlansIsNotImplementedInOss() {
    assertThrows<ApiNotImplementedInOssProblem> {
      entitlementsApiController.listAllEntitlementPlans()
    }
  }

  @Test
  fun testUpdateOrganizationEntitlementPlanIsNotImplementedInOss() {
    assertThrows<ApiNotImplementedInOssProblem> {
      entitlementsApiController.updateOrganizationEntitlementPlan(
        UpdateOrganizationEntitlementPlanRequestBody(
          organizationId = organizationId,
          planName = UpdateOrganizationEntitlementPlanRequestBody.PlanName.PRO,
        ),
      )
    }
  }
}
