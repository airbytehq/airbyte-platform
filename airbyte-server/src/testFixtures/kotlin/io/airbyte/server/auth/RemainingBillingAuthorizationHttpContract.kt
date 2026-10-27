/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.services.OrganizationService
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.Permission
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.BillingAuthorizationDataService
import io.airbyte.server.apis.controllers.BillingController
import io.airbyte.server.apis.controllers.BillingNotificationSettingsController
import io.airbyte.server.apis.controllers.OrganizationPaymentConfigController
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingBillingAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `billing rejects unsupported caller credentials before external billing or payment writes`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `every billing operation declares its exact edition policy and executes its normal response`() {
    for (scenario in scenarios()) exercise(scenario, status(scenario))
  }

  @Test
  fun `billing admin member and trial reader actions reject absent or insufficient grants before business`() {
    for (scenario in scenarios()) {
      exercise(scenario, 403, granted = false)
      if (scenario.grant != Permission.PermissionType.ORGANIZATION_MEMBER && scenario.grant != Permission.PermissionType.ORGANIZATION_READER) {
        exercise(scenario, 403, grant = Permission.PermissionType.ORGANIZATION_READER)
      }
    }
  }

  @Test
  fun `billing scope claims and required organization errors fail before business while configured no auth keeps stubs`() {
    for (scenario in scenarios()) {
      exercise(scenario, 403, token = "scoped:${UUID(0, 1)}")
      exercise(scenario, status(scenario, noAuth = true), granted = false, security = false)
      if (!scenario.global && !scenario.unavailable && (cloud || scenario.implementedInOss)) {
        exercise(scenario, 404, missing = true)
        exercise(scenario, 500, lookupError = true)
      }
    }
  }

  @Test
  fun `OSS billing stubs keep their action without reading nonexistent organization metadata`() {
    if (cloud) return
    assertAll(
      scenarios().filter { !it.implementedInOss && !it.unavailable }.map { scenario ->
        Executable {
          exercise(scenario, 501, missing = true, declaration = false, unusedOwner = true)
          exercise(scenario, 501, lookupError = true, declaration = false, unusedOwner = true)
          exercise(scenario, 501, missing = true, security = false, granted = false, declaration = false, unusedOwner = true)
          exercise(scenario, 403, granted = false, declaration = false, unusedOwner = true)
          exercise(scenario, 403, token = "workload", declaration = false, unusedOwner = true)
        }
      },
    )
  }

  @Test
  fun `OSS billing stubs reject malformed organization selectors before business`() {
    if (cloud) return
    assertAll(
      scenarios().filter { !it.global && !it.implementedInOss && !it.unavailable }.flatMap { scenario ->
        listOf("{}", """{"organizationId":null}""").flatMap { body ->
          val request = scenario.copy(body = body)
          listOf(
            Executable { exercise(request, 400, security = false, granted = false, unusedOwner = true) },
            Executable { exercise(request, 400, grant = Permission.PermissionType.INSTANCE_ADMIN, unusedOwner = true) },
            Executable { exercise(request, 400, unusedOwner = true) },
            Executable { exercise(request, 400, granted = false, unusedOwner = true) },
            Executable { exercise(request, 400, token = "workload", grant = Permission.PermissionType.INSTANCE_ADMIN, unusedOwner = true) },
            Executable {
              exercise(
                request,
                400,
                token = "scoped:${UUID(0, 1)}",
                grant = Permission.PermissionType.INSTANCE_ADMIN,
                unusedOwner = true,
              )
            },
          )
        }
      },
    )
  }

  @Test
  fun `OSS billing backfill rejects null entries before business`() {
    if (cloud) return
    val request = scenarios().single { it.path.endsWith("backfill_usage") }.copy(body = """{"jobIds":[null],"backfillId":null}""")
    assertAll(
      { exercise(request, 400, unusedOwner = true) },
      { exercise(request, 400, security = false, granted = false, unusedOwner = true) },
      { exercise(request, 400, grant = Permission.PermissionType.ORGANIZATION_ADMIN, unusedOwner = true) },
      { exercise(request, 400, token = "workload", unusedOwner = true) },
      { exercise(request, 400, token = "scoped:${UUID(0, 1)}", unusedOwner = true) },
    )
  }

  @Test
  fun `billing validates every backfill job before external ingestion and keeps empty global batches`() {
    val backfill = scenarios().single { it.path.endsWith("backfill_usage") }
    exercise(backfill.copy(body = """{"jobIds":[],"backfillId":"fixture"}"""), status(backfill))
    exercise(backfill, if (cloud) 404 else 501, missingJob = true)
    exercise(backfill, if (cloud) 500 else 501, jobLookupError = true)
    for (scenario in scenarios().filter { it.implementedInOss }) {
      exercise(scenario, 404, missingPayment = true)
      exercise(scenario, 500, paymentLookupError = true)
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    granted: Boolean = true,
    grant: Permission.PermissionType = scenario.grant,
    declaration: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    missingJob: Boolean = false,
    jobLookupError: Boolean = false,
    missingPayment: Boolean = false,
    paymentLookupError: Boolean = false,
    unusedOwner: Boolean = false,
  ) {
    val controllers =
      if (cloud) {
        listOf(
          Class.forName("io.airbyte.server.wrapped.apis.controllers.BillingWrappedController"),
          Class.forName("io.airbyte.server.wrapped.apis.controllers.BillingNotificationSettingsWrappedController"),
          Class.forName("io.airbyte.server.wrapped.apis.controllers.OrganizationPaymentConfigWrappedController"),
        )
      } else {
        listOf(BillingController::class.java, BillingNotificationSettingsController::class.java, OrganizationPaymentConfigController::class.java)
      }
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val billing = mockk<BillingAuthorizationDataService>()
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { it.rule }.toSet(), security).run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is OrganizationPaymentConfigService -> {
              every { instance.findByOrganizationId(ORGANIZATION) } returns paymentConfig()
              every { instance.savePaymentConfig(match { it.organizationId == ORGANIZATION }) } just Runs
            }
            is OrganizationService -> every { instance.handlePaymentGracePeriodEnded(OrganizationId(ORGANIZATION)) } just Runs
          }
          configureDependency(instance)
        }
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        if (missing) every { data.organization(ORGANIZATION) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.organization(ORGANIZATION) } throws java.io.IOException("billing organization unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("ORGANIZATION")) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        every { billing.hasPaymentConfiguration(ORGANIZATION) } returns !missingPayment
        if (paymentLookupError) every { billing.hasPaymentConfiguration(ORGANIZATION) } throws java.io.IOException("payment metadata unavailable")
        every { billing.backfillJobOwner(41) } returns WorkspaceAndOrganization(UUID(0, 1), ORGANIZATION)
        every { billing.backfillJobOwner(42) } returns WorkspaceAndOrganization(UUID(0, 5), UUID(0, 6))
        if (missingJob) every { billing.backfillJobOwner(42) } throws AuthorizationResourceNotFoundException()
        if (jobLookupError) every { billing.backfillJobOwner(42) } throws java.io.IOException("second historical job unavailable")
        context.registerSingleton(BillingAuthorizationDataService::class.java, billing)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, scenario.method, scenario.template, scenario.rule)
        val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.body()}")
        if (expected !in setOf(200, 204, 501)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (!cloud && !scenario.implementedInOss) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (unusedOwner) {
          verify { data wasNot Called }
          verify { billing wasNot Called }
        }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val grant: Permission.PermissionType,
    val method: String = "POST",
    val template: String = path,
    val global: Boolean = false,
    val unavailable: Boolean = false,
    val implementedInOss: Boolean = false,
    val cloudStatus: Int = 200,
  )

  private fun status(
    s: Scenario,
    noAuth: Boolean = false,
  ): Int =
    if (s.unavailable) {
      if (noAuth) 501 else 403
    } else if (cloud) {
      s.cloudStatus
    } else if (s.implementedInOss) {
      200
    } else {
      501
    }

  private fun scenarios(): List<Scenario> {
    val org = """{"organizationId":"$ORGANIZATION"}"""
    val admin = Permission.PermissionType.ORGANIZATION_ADMIN
    val global = Permission.PermissionType.INSTANCE_ADMIN
    val manage = if (cloud) "io.airbyte.server.auth.BillingManageRule" else "io.airbyte.server.auth.BillingManageUnsupportedRule"
    return listOf(
      Scenario("/api/v1/billing/customer_portal", """{"organizationId":"$ORGANIZATION","returnUrl":"https://example.com/billing"}""", manage, admin),
      Scenario("/api/v1/billing/list_invoices", org, manage, admin),
      Scenario("/api/v1/billing/payment_information", org, manage, admin),
      Scenario(
        "/api/v1/billing/subscription_info",
        org,
        if (cloud) "io.airbyte.server.auth.BillingSubscriptionRule" else "io.airbyte.server.auth.BillingSubscriptionUnsupportedRule",
        Permission.PermissionType.ORGANIZATION_MEMBER,
      ),
      Scenario(
        "/api/v1/billing/trial_status",
        org,
        if (cloud) "io.airbyte.server.auth.BillingTrialRule" else "io.airbyte.server.auth.BillingTrialUnsupportedRule",
        Permission.PermissionType.ORGANIZATION_READER,
      ),
      Scenario("/api/v1/billing/cancel_subscription", org, manage, admin),
      Scenario("/api/v1/billing/unschedule_cancel_subscription", org, manage, admin, cloudStatus = 204),
      Scenario("/api/v1/billing/unschedule_plan_change", org, manage, admin, cloudStatus = 204),
      Scenario("/api/v1/billing/notification_settings/get", org, manage, admin),
      Scenario(
        "/api/v1/billing/notification_settings/update",
        """{"organizationId":"$ORGANIZATION","syncCreditThreshold":0,"invoiceAmountThreshold":0}""",
        manage,
        admin,
      ),
      Scenario(
        "/api/v1/billing/backfill_usage",
        """{"jobIds":[41,42],"backfillId":"fixture"}""",
        if (cloud) "io.airbyte.server.wrapped.auth.BillingBackfillRule" else "io.airbyte.server.auth.BillingBackfillStubRule",
        global,
        global = true,
      ),
      Scenario("/api/v1/billing/disable_delinquent_workspaces", "{}", "io.airbyte.server.auth.GlobalAdminRule", global, global = true),
      Scenario(
        "/api/v1/organization_payment_config/$ORGANIZATION",
        "",
        "io.airbyte.server.auth.OrganizationPaymentConfigRule",
        global,
        method = "GET",
        template = "/api/v1/organization_payment_config/{organizationId}",
        implementedInOss = true,
      ),
      Scenario(
        "/api/v1/organization_payment_config/$ORGANIZATION/end_grace_period",
        "{}",
        "io.airbyte.server.auth.OrganizationPaymentConfigRule",
        global,
        template = "/api/v1/organization_payment_config/{organizationId}/end_grace_period",
        implementedInOss = true,
      ),
      Scenario(
        "/api/v1/organization_payment_config",
        """{"organizationId":"$ORGANIZATION","paymentProviderId":"fixture","paymentStatus":"manual"}""",
        "io.airbyte.server.auth.OrganizationPaymentConfigRule",
        global,
        implementedInOss = true,
      ),
    ) +
      if (cloud) {
        emptyList()
      } else {
        listOf(
          Scenario(
            "/api/v1/billing/handle_webhook",
            "{}",
            "io.airbyte.server.auth.UnavailableProtectedRule",
            global,
            global = true,
            unavailable = true,
          ),
        )
      }
  }

  companion object {
    val ORGANIZATION: UUID = UUID(0, 2)

    fun paymentConfig(): OrganizationPaymentConfig =
      OrganizationPaymentConfig()
        .withOrganizationId(ORGANIZATION)
        .withPaymentProviderId("fixture")
        .withPaymentStatus(OrganizationPaymentConfig.PaymentStatus.MANUAL)
        .withSubscriptionStatus(OrganizationPaymentConfig.SubscriptionStatus.SUBSCRIBED)
  }
}
