/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.sso

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger { }

/**
 * Resolves the RBAC entitlement for latency-sensitive SSO provisioning paths.
 *
 * The Stigg client can block well beyond an acceptable login budget. Run the lookup off the caller
 * thread and treat a timeout or failure as indeterminate so only a definitive denial can cause SSO
 * provisioning to default to organization admin.
 */
@Singleton
class SsoRbacEntitlementChecker(
  private val entitlementService: EntitlementService,
) {
  private val executor: ExecutorService =
    Executors.newCachedThreadPool { runnable ->
      Thread(runnable, "sso-rbac-entitlement-check").apply { isDaemon = true }
    }

  fun check(organizationId: OrganizationId): EntitlementResult = checkWithinBudget(organizationId, CHECK_BUDGET)

  internal fun checkWithinBudget(
    organizationId: OrganizationId,
    budget: Duration,
  ): EntitlementResult {
    val check =
      executor.submit<EntitlementResult> {
        entitlementService.checkEntitlement(organizationId, RbacRolesEntitlement)
      }
    return try {
      check.get(budget.toMillis(), TimeUnit.MILLISECONDS)
    } catch (e: TimeoutException) {
      check.cancel(true)
      logger.warn {
        "RBAC entitlement check exceeded ${budget.toMillis()}ms for organization ${organizationId.value}; " +
          "treating the result as indeterminate"
      }
      indeterminateResult()
    } catch (e: InterruptedException) {
      check.cancel(true)
      Thread.currentThread().interrupt()
      indeterminateResult()
    } catch (e: ExecutionException) {
      logger.warn(e.cause ?: e) {
        "RBAC entitlement check failed for organization ${organizationId.value}; treating the result as indeterminate"
      }
      indeterminateResult()
    }
  }

  private fun indeterminateResult(): EntitlementResult =
    EntitlementResult(
      featureId = RbacRolesEntitlement.featureId,
      isEntitled = false,
      isEntitlementCheckSuccessful = false,
    )

  private companion object {
    private val CHECK_BUDGET: Duration = Duration.ofSeconds(3)
  }
}
