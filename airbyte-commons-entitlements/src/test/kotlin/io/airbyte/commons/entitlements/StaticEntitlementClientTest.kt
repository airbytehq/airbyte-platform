/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.CommittedDataWorkersEntitlement
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class StaticEntitlementClientTest {
  private val organizationId = OrganizationId(UUID.randomUUID())

  // Deny-all mode: empty grant set (the default).
  private val denyingClient = StaticEntitlementClient()

  @Test
  fun `deny mode - checkEntitlement denies`() {
    val result = denyingClient.checkEntitlement(organizationId, MappersEntitlement)

    assertEquals(
      EntitlementResult(
        featureId = MappersEntitlement.featureId,
        isEntitled = false,
        reason = "StaticEntitlementClient: entitlement is not statically granted",
      ),
      result,
    )
  }

  @Test
  fun `deny mode - getEntitlements returns empty list`() {
    val result = denyingClient.getEntitlements(organizationId)
    assertEquals(emptyList<EntitlementResult>(), result)
  }

  @Test
  fun `deny mode - getNumericEntitlement returns no access`() {
    val result = denyingClient.getNumericEntitlement(organizationId, FeatureEntitlement("feature-committed-data-workers"))

    assertEquals(
      NumericEntitlementResult(
        featureId = "feature-committed-data-workers",
        hasAccess = false,
        value = null,
        reason = "StaticEntitlementClient: entitlement is not statically granted",
      ),
      result,
    )
  }

  @Test
  fun `deny mode - getEntitlementsForPlan returns empty list`() {
    assertEquals(emptyList<Entitlement>(), denyingClient.getEntitlementsForPlan(EntitlementPlan.STANDARD))
  }

  @Test
  fun `deny mode - getPlans returns empty list`() {
    assertEquals(emptyList<EntitlementPlanResponse>(), denyingClient.getPlans(organizationId))
  }

  @Test
  fun `addOrganization does nothing`() {
    // should not throw
    val organizationId = OrganizationId(UUID.randomUUID())
    denyingClient.addOrganization(organizationId, EntitlementPlan.STANDARD)
  }

  @Test
  fun `updateOrganization does nothing`() {
    // should not throw
    val organizationId = OrganizationId(UUID.randomUUID())
    denyingClient.updateOrganization(organizationId, EntitlementPlan.STANDARD)
  }

  // Partial grant mode: two ids granted, everything else denied.
  private val grantingClient =
    StaticEntitlementClient(
      grantedFeatureIds = setOf(MappersEntitlement.featureId, CommittedDataWorkersEntitlement.featureId),
    )

  @Test
  fun `grant mode - checkEntitlement grants granted ids and denies others`() {
    val granted = grantingClient.checkEntitlement(organizationId, MappersEntitlement)
    assertTrue(granted.isEntitled)
    assertEquals("StaticEntitlementClient: entitlement is statically granted", granted.reason)

    val denied = grantingClient.checkEntitlement(organizationId, FeatureEntitlement("feature-sso"))
    assertFalse(denied.isEntitled)
    assertEquals("StaticEntitlementClient: entitlement is not statically granted", denied.reason)
  }

  @Test
  fun `grant mode - getEntitlements covers all entitlements with per-id truth values and feature names`() {
    val result = grantingClient.getEntitlements(organizationId)

    assertEquals(Entitlements.all.map { it.featureId }, result.map { it.featureId })
    result.forEach { entitlementResult ->
      assertEquals(
        entitlementResult.featureId in setOf(MappersEntitlement.featureId, CommittedDataWorkersEntitlement.featureId),
        entitlementResult.isEntitled,
      )
      assertEquals(Entitlements.all.first { it.featureId == entitlementResult.featureId }.name, entitlementResult.featureName)
    }
  }

  @Test
  fun `grant mode - granted numeric entitlement returns unlimited access`() {
    val result = grantingClient.getNumericEntitlement(organizationId, CommittedDataWorkersEntitlement)

    assertEquals(
      NumericEntitlementResult(
        featureId = CommittedDataWorkersEntitlement.featureId,
        hasAccess = true,
        value = null,
        isUnlimited = true,
        reason = "StaticEntitlementClient: entitlement is statically granted",
      ),
      result,
    )
  }

  @Test
  fun `grant mode - denied numeric entitlement returns no access`() {
    val result = grantingClient.getNumericEntitlement(organizationId, FeatureEntitlement("feature-privatelink-limit"))

    assertFalse(result.hasAccess)
    assertNull(result.value)
    assertFalse(result.isUnlimited)
  }

  @Test
  fun `grant mode - getEntitlementsForPlan returns only granted entitlements`() {
    val result = grantingClient.getEntitlementsForPlan(EntitlementPlan.STANDARD)

    assertEquals(
      setOf(MappersEntitlement.featureId, CommittedDataWorkersEntitlement.featureId),
      result.map { it.featureId }.toSet(),
    )
  }

  @Test
  fun `grant mode - getPlans returns empty list`() {
    assertEquals(emptyList<EntitlementPlanResponse>(), grantingClient.getPlans(organizationId))
  }

  @Test
  fun `grant mode - addOrganization and updateOrganization do nothing`() {
    // should not throw
    grantingClient.addOrganization(organizationId, EntitlementPlan.STANDARD)
    grantingClient.updateOrganization(organizationId, EntitlementPlan.STANDARD)
  }

  // Numeric grant mode: finite values for numeric entitlements, plus a configured plan.
  private val numericClient =
    StaticEntitlementClient(
      grantedFeatureIds = setOf(MappersEntitlement.featureId),
      numericEntitlementValues = mapOf(CommittedDataWorkersEntitlement.featureId to 3L),
      plan = EntitlementPlan.PLUS,
    )

  @Test
  fun `numeric mode - numeric entitlement returns the configured finite value`() {
    val result = numericClient.getNumericEntitlement(organizationId, CommittedDataWorkersEntitlement)

    assertEquals(
      NumericEntitlementResult(
        featureId = CommittedDataWorkersEntitlement.featureId,
        hasAccess = true,
        value = 3L,
        isUnlimited = false,
        reason = "StaticEntitlementClient: entitlement is statically granted",
      ),
      result,
    )
  }

  @Test
  fun `numeric mode - numeric value takes precedence over a boolean grant`() {
    val client =
      StaticEntitlementClient(
        grantedFeatureIds = setOf(CommittedDataWorkersEntitlement.featureId),
        numericEntitlementValues = mapOf(CommittedDataWorkersEntitlement.featureId to 2L),
      )
    val result = client.getNumericEntitlement(organizationId, CommittedDataWorkersEntitlement)

    assertTrue(result.hasAccess)
    assertEquals(2L, result.value)
    assertFalse(result.isUnlimited)
  }

  @Test
  fun `numeric mode - a numeric value also counts as granted for boolean checks`() {
    assertTrue(numericClient.checkEntitlement(organizationId, CommittedDataWorkersEntitlement).isEntitled)
  }

  @Test
  fun `numeric mode - zero is a valid finite value`() {
    val client =
      StaticEntitlementClient(
        numericEntitlementValues = mapOf(CommittedDataWorkersEntitlement.featureId to 0L),
      )
    val result = client.getNumericEntitlement(organizationId, CommittedDataWorkersEntitlement)

    assertTrue(result.hasAccess)
    assertEquals(0L, result.value)
    assertFalse(result.isUnlimited)
  }

  @Test
  fun `numeric mode - getPlans returns the configured plan`() {
    assertEquals(
      listOf(EntitlementPlanResponse(planEnum = EntitlementPlan.PLUS, planId = EntitlementPlan.PLUS.id, planName = EntitlementPlan.PLUS.displayName)),
      numericClient.getPlans(organizationId),
    )
  }

  @Test
  fun `numeric mode - getEntitlements surfaces the numeric value`() {
    val result = numericClient.getEntitlements(organizationId)

    val committedDataWorkers = result.first { it.featureId == CommittedDataWorkersEntitlement.featureId }
    assertEquals(true, committedDataWorkers.isEntitled)
    assertEquals(3L, committedDataWorkers.value)
    assertEquals(false, committedDataWorkers.isUnlimited)

    val mappers = result.first { it.featureId == MappersEntitlement.featureId }
    assertEquals(true, mappers.isEntitled)
    assertEquals(null, mappers.value)
    assertEquals(false, mappers.isUnlimited)
  }
}
