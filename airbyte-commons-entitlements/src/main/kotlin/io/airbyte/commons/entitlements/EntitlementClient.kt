/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementPlan
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.data.services.OrganizationService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.stigg.api.client.Stigg
import io.stigg.api.operations.GetEntitlementQuery
import io.stigg.api.operations.GetEntitlementsQuery
import io.stigg.api.operations.ProvisionCustomerMutation
import io.stigg.api.operations.type.FetchEntitlementQuery
import io.stigg.api.operations.type.FetchEntitlementsQuery
import io.stigg.api.operations.type.ProvisionCustomerInput
import io.stigg.api.operations.type.ProvisionCustomerSubscriptionInput
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

interface EntitlementClient {
  fun checkEntitlement(
    organizationId: UUID,
    entitlement: Entitlement,
  ): EntitlementResult

  fun getEntitlements(organizationId: UUID): List<EntitlementResult>

  fun addOrganizationToPlan(
    organizationId: UUID,
    plan: EntitlementPlan?,
  )
}

@Singleton
class DefaultEntitlementClient : EntitlementClient {
  override fun checkEntitlement(
    organizationId: UUID,
    entitlement: Entitlement,
  ): EntitlementResult =
    EntitlementResult(featureId = entitlement.featureId, isEntitled = false, reason = "DefaultEntitlementClient grants no entitlements")

  override fun getEntitlements(organizationId: UUID): List<EntitlementResult> = emptyList()

  override fun addOrganizationToPlan(
    organizationId: UUID,
    plan: EntitlementPlan?,
  ) {}
}

@Factory
class StiggClientFactory {
  @Singleton
  fun stiggClient(
    @Value("\${airbyte.entitlement.stigg.api-key}") serverKey: String,
  ): Stigg = Stigg.createClient(serverKey)
}

@Singleton
@Replaces(DefaultEntitlementClient::class)
@Requires(property = "airbyte.entitlement.client", value = "stigg")
class StiggEntitlementClient(
  private val stigg: Stigg,
  private val organizationService: OrganizationService,
) : EntitlementClient {
  override fun checkEntitlement(
    organizationId: UUID,
    entitlement: Entitlement,
  ): EntitlementResult {
    logger.debug { "Checking entitlement organizationId=$organizationId entitlement=$entitlement" }

    val result =
      stigg.query(
        GetEntitlementQuery
          .builder()
          .query(
            FetchEntitlementQuery
              .builder()
              .customerId(organizationId.toString())
              .featureId(entitlement.featureId)
              .build(),
          ).build(),
      )

    logger.debug {
      "Got entitlement organizationId=$organizationId entitlement=$entitlement isGranted=${result.entitlement.entitlementFragment.isGranted} accessDeniedReason=${result.entitlement.entitlementFragment.accessDeniedReason?.rawValue}"
    }

    return EntitlementResult(
      entitlement.featureId,
      result.entitlement.entitlementFragment.isGranted,
      result.entitlement.entitlementFragment.accessDeniedReason
        ?.rawValue,
    )
  }

  override fun getEntitlements(organizationId: UUID): List<EntitlementResult> {
    logger.debug { "Getting entitlements organizationId=$organizationId" }

    val result =
      stigg.query(
        GetEntitlementsQuery
          .builder()
          .query(
            FetchEntitlementsQuery
              .builder()
              .customerId(organizationId.toString())
              .build(),
          ).build(),
      )

    logger.debug {
      "Got entitlements organizationId=$organizationId result=$result"
    }

    return result.entitlements.map {
      EntitlementResult(
        it.entitlementFragment.feature.featureFragment.refId,
        it.entitlementFragment.isGranted,
        it.entitlementFragment.accessDeniedReason ?.rawValue,
      )
    }
  }

  override fun addOrganizationToPlan(
    organizationId: UUID,
    plan: EntitlementPlan?,
  ) {
    val org =
      organizationService
        .getOrganization(organizationId)
        ?.orElseThrow {
          IllegalStateException("Organization $organizationId not found; could not add to plan ${plan ?: "null"}")
        } ?: throw IllegalStateException("getOrganization() returned null for $organizationId; could not add to plan ${plan ?: "null"}")

    val input =
      ProvisionCustomerInput
        .builder()
        .customerId(organizationId.toString())
        .additionalMetaData(mapOf("name" to org.name))

    plan?.let {
      input.subscriptionParams(
        ProvisionCustomerSubscriptionInput(
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
          plan.id,
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
        ),
      )
    }

    stigg.mutation(ProvisionCustomerMutation(input.build()))

    logger.info { "Added organization to plan. organizationId=$organizationId plan=$plan" }
  }
}
