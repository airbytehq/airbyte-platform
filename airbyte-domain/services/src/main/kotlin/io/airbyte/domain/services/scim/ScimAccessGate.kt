/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.entitlements.models.ScimEntitlement
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ScimProvisioningPilot
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger { }

@Singleton
class ScimAccessGate(
  private val entitlementService: EntitlementService,
  private val featureFlagClient: FeatureFlagClient,
) {
  private val organizationInfoCache: Cache<UUID, Boolean> =
    Caffeine
      .newBuilder()
      .expireAfterWrite(ORGANIZATION_INFO_CACHE_TTL)
      .maximumSize(ORGANIZATION_INFO_CACHE_MAX_SIZE)
      .build()

  private val advisoryExecutor: ExecutorService =
    Executors.newCachedThreadPool { runnable ->
      Thread(runnable, "scim-access-gate-advisory").apply { isDaemon = true }
    }

  fun isAllowed(organizationId: OrganizationId): Boolean {
    if (!entitlementService.checkEntitlement(organizationId, ScimEntitlement).isEntitled) {
      return false
    }
    if (!entitlementService.checkEntitlement(organizationId, GroupsEntitlement).isEntitled) {
      return false
    }
    return featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
  }

  /**
   * Cached variant for the advisory `scim` flag on OrganizationInfoRead. Both org-info endpoints
   * serve that flag on the webapp's boot path, where an uncached call puts a Stigg lookup (30s
   * timeout) in front of a suspense query for every request from an organization that has SCIM
   * enabled. The flag only decides whether the members page renders as provider-managed, so an
   * answer that is stale by at most one TTL is acceptable. [EntitlementService.checkEntitlement]
   * degrades to "not entitled" rather than throwing, so a Stigg outage caches `false` and stops
   * repeating the stall.
   *
   * Use [isAllowed], never this method, for authentication and write authorization: those paths
   * must observe a revoked entitlement or a disabled pilot flag immediately.
   */
  fun isAllowedForOrganizationInfo(organizationId: OrganizationId): Boolean =
    organizationInfoCache.get(organizationId.value) { evaluateWithinBudget(organizationId) }

  /**
   * Runs [isAllowed] off the request thread and abandons it after [ORGANIZATION_INFO_BUDGET].
   * [EntitlementService.checkEntitlement] converts a Stigg failure into "not entitled", but it
   * cannot shorten one: a hung sidecar blocks for the full 30s Stigg timeout, which on this path
   * suspends the webapp's sidebar. Abandoning the lookup reports the flag as disabled, matching
   * what a Stigg error already produces.
   *
   * The abandoned result is cached like any other, so an outage costs one wait per organization
   * per TTL rather than one per request. The trade is that a transient stall renders the members
   * page as unmanaged for up to one TTL; the identity provider still reconciles any edits made in
   * that window.
   */
  private fun evaluateWithinBudget(organizationId: OrganizationId): Boolean {
    val evaluation = advisoryExecutor.submit<Boolean> { isAllowed(organizationId) }
    return try {
      evaluation.get(ORGANIZATION_INFO_BUDGET.toMillis(), TimeUnit.MILLISECONDS)
    } catch (e: TimeoutException) {
      evaluation.cancel(true)
      logger.warn {
        "SCIM access gate exceeded ${ORGANIZATION_INFO_BUDGET.toMillis()}ms for organization " +
          "${organizationId.value}; reporting SCIM as disabled"
      }
      false
    } catch (e: InterruptedException) {
      evaluation.cancel(true)
      Thread.currentThread().interrupt()
      false
    }
  }

  private companion object {
    private val ORGANIZATION_INFO_CACHE_TTL: Duration = Duration.ofSeconds(60)
    private const val ORGANIZATION_INFO_CACHE_MAX_SIZE = 10_000L
    private val ORGANIZATION_INFO_BUDGET: Duration = Duration.ofSeconds(3)
  }
}
