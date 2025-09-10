/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.micronaut.runtime.AirbyteEntitlementConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.stigg.api.client.Stigg
import io.stigg.api.operations.GetActiveSubscriptionsListQuery
import io.stigg.api.operations.GetEntitlementQuery
import io.stigg.api.operations.GetEntitlementsQuery
import io.stigg.api.operations.ProvisionCustomerMutation
import io.stigg.api.operations.type.FetchEntitlementQuery
import io.stigg.api.operations.type.FetchEntitlementsQuery
import io.stigg.api.operations.type.GetActiveSubscriptionsInput
import io.stigg.api.operations.type.ProvisionCustomerInput
import io.stigg.api.operations.type.ProvisionCustomerSubscriptionInput
import jakarta.inject.Singleton

/**
 * EntitlementClient is an internal interface that represents
 * the functions needed to manage entitlements and plans in Stigg.
 *
 * Note that entitlements are currently implemented in different ways,
 * for example some are implemented as Launch Darkly feature flags,
 * so EntitlementClient does not (yet) capture all forms of entitlements.
 */
internal interface EntitlementClient {
  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult

  fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult>

  fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  )
}
