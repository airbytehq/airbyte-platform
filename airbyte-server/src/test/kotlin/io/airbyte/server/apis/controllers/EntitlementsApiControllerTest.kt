/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.GetEntitlementsByOrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationIsEntitledRequestBody
import io.airbyte.api.model.generated.UpdateOrganizationEntitlementPlanRequestBody
import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceInvalidOrganizationStateProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class EntitlementsApiControllerTest {
  private lateinit var entitlementService: EntitlementService
  private lateinit var organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper
  private lateinit var entitlementsApiController: EntitlementsApiController

  private val organizationId = UUID.randomUUID()
  private val featureId = "feature-sso"

  @BeforeEach
  fun setup() {
    entitlementService = mockk()
    organizationAccessAuthorizationHelper = mockk(relaxed = true)
    entitlementsApiController = EntitlementsApiController(entitlementService, organizationAccessAuthorizationHelper)
  }

  @Test
  fun testCheckEntitlement() {
    val requestBody = OrganizationIsEntitledRequestBody().organizationId(organizationId).featureId(featureId)
    val entitlementResult = EntitlementResult(featureId, true, null)

    every { entitlementService.checkEntitlement(OrganizationId(organizationId), any()) } returns entitlementResult

    val result = entitlementsApiController.checkEntitlement(requestBody)

    assertNotNull(result)
    assertEquals(featureId, result!!.featureId)
    assertEquals(true, result.isEntitled)
    verify(exactly = 1) { entitlementService.checkEntitlement(OrganizationId(organizationId), any()) }
  }

  @Test
  fun testCheckEntitlementThrowsOnUnknownEntitlement() {
    val requestBody = OrganizationIsEntitledRequestBody().organizationId(organizationId).featureId("not-a-featuer")
    val entitlementResult = EntitlementResult(featureId, true, null)

    every { entitlementService.checkEntitlement(OrganizationId(organizationId), any()) } returns entitlementResult

    assertThrows<IllegalArgumentException> { entitlementsApiController.checkEntitlement(requestBody) }
  }

  @Test
  fun testGetEntitlementsSucceedsWhenValidationPasses() {
    val requestBody = GetEntitlementsByOrganizationIdRequestBody().organizationId(organizationId)
    val entitlementResults =
      listOf(
        EntitlementResult("feature1", true, null),
        EntitlementResult("feature2", false, "Insufficient plan"),
      )

    // Mock validation to succeed
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } returns Unit
    every { entitlementService.getEntitlements(OrganizationId(organizationId)) } returns entitlementResults

    val result = entitlementsApiController.getEntitlements(requestBody)

    assertNotNull(result)
    assertEquals(2, result!!.entitlements.size)

    val firstEntitlement = result.entitlements[0]
    assertEquals("feature1", firstEntitlement.featureId)
    assertEquals(true, firstEntitlement.isEntitled)
    assertEquals(null, firstEntitlement.accessDeniedReason)

    val secondEntitlement = result.entitlements[1]
    assertEquals("feature2", secondEntitlement.featureId)
    assertEquals(false, secondEntitlement.isEntitled)
    assertEquals("Insufficient plan", secondEntitlement.accessDeniedReason)

    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
    verify(exactly = 1) { entitlementService.getEntitlements(OrganizationId(organizationId)) }
  }

  @Test
  fun testGetEntitlementsReturnsEmptyListWhenValidationFails() {
    val requestBody = GetEntitlementsByOrganizationIdRequestBody().organizationId(organizationId)
    val forbiddenException = ForbiddenProblem()

    // Mock validation to fail
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } throws forbiddenException

    val result = entitlementsApiController.getEntitlements(requestBody)

    assertNotNull(result)
    assertEquals(0, result!!.entitlements.size) // Should return empty list instead of throwing
    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
    verify(exactly = 0) { entitlementService.getEntitlements(any()) } // Should not call service
  }

  @Test
  fun testGetEntitlementsWithEmptyResults() {
    val requestBody = GetEntitlementsByOrganizationIdRequestBody().organizationId(organizationId)

    // Mock validation to succeed
    every { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) } returns Unit
    every { entitlementService.getEntitlements(OrganizationId(organizationId)) } returns emptyList()

    val result = entitlementsApiController.getEntitlements(requestBody)

    assertNotNull(result)
    assertEquals(0, result!!.entitlements.size)

    verify(exactly = 1) { organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }
    verify(exactly = 1) { entitlementService.getEntitlements(OrganizationId(organizationId)) }
  }

  @Test
  fun testListAllEntitlementPlans() {
    val result = entitlementsApiController.listAllEntitlementPlans()

    assertNotNull(result)
    assertEquals(11, result!!.plans.size)
    assertTrue(result.plans.any { it.planName == "STANDARD" })
  }

  @Test
  fun testUpdateOrganizationEntitlementPlanSuccess() {
    val requestBody =
      UpdateOrganizationEntitlementPlanRequestBody()
        .organizationId(organizationId)
        .planName(UpdateOrganizationEntitlementPlanRequestBody.PlanNameEnum.PRO)

    every { entitlementService.addOrUpdateOrganization(any(), any()) } returns Unit
    val result = entitlementsApiController.updateOrganizationEntitlementPlan(requestBody)

    assertNotNull(result)
    assertEquals(organizationId, result!!.organizationId)
    assertEquals("PRO", result.planName)
    verify(exactly = 1) { entitlementService.addOrUpdateOrganization(OrganizationId(organizationId), EntitlementPlan.PRO) }
  }

  @Test
  fun testUpdateOrganizationEntitlementPlan_MultiplePlansError() {
    val requestBody =
      UpdateOrganizationEntitlementPlanRequestBody()
        .organizationId(organizationId)
        .planName(UpdateOrganizationEntitlementPlanRequestBody.PlanNameEnum.STANDARD)

    val problemData =
      ProblemEntitlementServiceData()
        .organizationId(organizationId)
        .errorMessage("More than one entitlement plan found")
    val exception = EntitlementServiceInvalidOrganizationStateProblem(problemData)

    every { entitlementService.addOrUpdateOrganization(any(), any()) } throws exception

    val thrownException =
      assertThrows<EntitlementServiceInvalidOrganizationStateProblem> {
        entitlementsApiController.updateOrganizationEntitlementPlan(requestBody)
      }
    val errorData = thrownException.problem.getData() as ProblemEntitlementServiceData
    assertEquals("More than one entitlement plan found", errorData.errorMessage)
    assertEquals(organizationId, errorData.organizationId)

    verify(exactly = 1) {
      entitlementService.addOrUpdateOrganization(
        OrganizationId(organizationId),
        EntitlementPlan.STANDARD,
      )
    }
  }
}
