/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.services.OrganizationService
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.airbyte.domain.models.ServiceAccount
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.airbyte.server.services.DataplaneService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID

private const val SPEC = "BillingAndDataplaneApisSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Calls every billing, entitlement, payment config, data worker, private link and dataplane route over
 * HTTP with security enabled, at the exact path clients call. A route moved by the controller's
 * generated interface fails here rather than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class BillingAndDataplaneApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun dataplaneService(): DataplaneService =
      mockk(relaxed = true) {
        every { createNewDataplane(any(), any()) } returns
          DataplaneWithServiceAccount(dataplane(), ServiceAccount(id = UUID.randomUUID(), name = "test", secret = "secret"))
        every { updateDataplane(any(), any(), any()) } returns dataplane()
        every { deleteDataplane(any()) } returns dataplane()
        every { getDataplane(any()) } returns dataplane()
        every { getDataplaneByServiceAccountId(any()) } returns dataplane()
        every { listDataplanes(any<UUID>()) } returns listOf(dataplane())
        every { listDataplanes(any<List<UUID>>()) } returns emptyList()
        every { getToken(any(), any()) } returns "token"
      }

    @Singleton
    @Primary
    fun dataplaneGroupService(): DataplaneGroupService =
      mockk(relaxed = true) {
        every { getOrganizationIdFromDataplaneGroup(any()) } returns UUID.randomUUID()
        every { getDataplaneGroup(any()) } returns dataplaneGroup()
        every { writeDataplaneGroup(any()) } returns dataplaneGroup()
        every { listDataplaneGroups(any(), any()) } returns listOf(dataplaneGroup())
      }

    @Singleton
    @Primary
    fun dataplaneHealthService(): DataplaneHealthService =
      mockk(relaxed = true) {
        every { getDataplaneHealthInfos(any()) } returns emptyList()
      }

    // the dataplane initialize and heartbeat routes check the dataplane role through the resolver
    @Singleton
    @Primary
    fun roleResolver(): RoleResolver = mockk(relaxed = true)

    @Singleton
    @Primary
    fun entitlementService(): EntitlementService =
      mockk(relaxed = true) {
        every { checkEntitlement(any(), any()) } returns EntitlementResult(featureId = "feature-self-managed-regions", isEntitled = true)
        every { getEntitlements(any()) } returns emptyList()
      }

    @Singleton
    @Primary
    fun organizationAccessAuthorizationHelper(): OrganizationAccessAuthorizationHelper = mockk(relaxed = true)

    @Singleton
    @Primary
    fun organizationService(): OrganizationService = mockk(relaxed = true)

    @Singleton
    @Primary
    fun organizationPaymentConfigService(): OrganizationPaymentConfigService =
      mockk(relaxed = true) {
        every { findByOrganizationId(any()) } returns
          OrganizationPaymentConfig().apply {
            organizationId = UUID.randomUUID()
            paymentStatus = OrganizationPaymentConfig.PaymentStatus.OKAY
            subscriptionStatus = OrganizationPaymentConfig.SubscriptionStatus.SUBSCRIBED
          }
      }
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route answers its role with the expected status`(route: Route) {
    val status = statusOf(route.request(), route.allowedRole)
    assertThat(status)
      .withFailMessage { "$route with ${route.allowedRole} returned $status, expected ${route.allowedStatus}" }
      .isEqualTo(route.allowedStatus)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("securedRoutes")
  fun `route is forbidden without its role`(route: Route) {
    assertThat(statusOf(route.request(), AuthRoleConstants.AUTHENTICATED_USER)).isEqualTo(HttpStatus.FORBIDDEN)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routesWithRequiredFields")
  fun `missing required field is a client error`(route: Route) {
    val status = statusOf(route.request(body = emptyMap()), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "$route with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("implementedRoutesWithBody")
  fun `missing body is a client error`(route: Route) {
    val status = statusOf(HttpRequest.POST<Any>(route.path, null).contentType(MediaType.APPLICATION_JSON_TYPE), route.allowedRole)
    assertThat(status)
      .withFailMessage { "$route with no body returned $status" }
      .isEqualTo(HttpStatus.BAD_REQUEST)
  }

  private fun statusOf(
    request: MutableHttpRequest<*>,
    role: String,
  ): HttpStatus =
    try {
      client.toBlocking().exchange(request.header(TEST_ROLES_HEADER, role), Any::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }

  /**
   * @param allowedStatus what the route answers a caller with [allowedRole]; OSS stubs answer 501
   * @param secured whether a caller with only [AuthRoleConstants.AUTHENTICATED_USER] is rejected
   */
  data class Route(
    val path: String,
    val allowedRole: String,
    val body: Map<String, Any>,
    val allowedStatus: HttpStatus = HttpStatus.OK,
    val secured: Boolean = allowedRole != AuthRoleConstants.AUTHENTICATED_USER,
    val isGet: Boolean = false,
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> =
      if (isGet) HttpRequest.GET<Any>(path) else HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)

    override fun toString() = "${if (isGet) "GET" else "POST"} $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()
    private val orgId = mapOf("organizationId" to id)
    private val snakeOrgId = mapOf("organization_id" to id)

    private const val ORG_ADMIN = AuthRoleConstants.ORGANIZATION_ADMIN
    private const val ADMIN = AuthRoleConstants.ADMIN
    private val notInOss = HttpStatus.NOT_IMPLEMENTED

    private val routes =
      listOf(
        Route("/api/v1/billing/customer_portal", ORG_ADMIN, orgId + ("returnUrl" to "https://airbyte.com"), notInOss),
        Route("/api/v1/billing/list_invoices", ORG_ADMIN, orgId, notInOss),
        Route("/api/v1/billing/payment_information", ORG_ADMIN, orgId, notInOss),
        Route("/api/v1/billing/subscription_info", AuthRoleConstants.ORGANIZATION_MEMBER, orgId, notInOss),
        // OSS gives the webhook no security rule, so every caller is rejected; Cloud opens it to anonymous callers
        Route("/api/v1/billing/handle_webhook", AuthRoleConstants.AUTHENTICATED_USER, emptyMap(), HttpStatus.FORBIDDEN),
        Route("/api/v1/billing/trial_status", AuthRoleConstants.ORGANIZATION_READER, orgId, notInOss),
        Route("/api/v1/billing/backfill_usage", ADMIN, mapOf("jobIds" to listOf(1L), "backfillId" to "backfill"), notInOss),
        Route("/api/v1/billing/cancel_subscription", ORG_ADMIN, orgId, notInOss),
        Route("/api/v1/billing/disable_delinquent_workspaces", ADMIN, emptyMap(), notInOss),
        Route("/api/v1/billing/unschedule_cancel_subscription", ORG_ADMIN, orgId, notInOss),
        Route("/api/v1/billing/unschedule_plan_change", ORG_ADMIN, orgId, notInOss),
        Route("/api/v1/billing/notification_settings/get", ORG_ADMIN, orgId, notInOss),
        Route(
          "/api/v1/billing/notification_settings/update",
          ORG_ADMIN,
          orgId + mapOf("syncCreditThreshold" to 1, "invoiceAmountThreshold" to 1),
          notInOss,
        ),
        Route("/api/v1/data_worker_allocation/list", ORG_ADMIN, snakeOrgId, notInOss),
        Route("/api/v1/data_worker_allocation/get", ORG_ADMIN, snakeOrgId + ("dataplane_group_id" to id), notInOss),
        Route(
          "/api/v1/data_worker_allocation/reallocate",
          ORG_ADMIN,
          snakeOrgId + mapOf("from_dataplane_group_id" to id, "to_dataplane_group_id" to id, "amount" to 1.0),
          notInOss,
        ),
        Route("/api/v1/data_worker_allocation/add_capacity", ADMIN, snakeOrgId + ("amount" to 1.0), notInOss),
        Route("/api/v1/data_worker_allocation/remove_capacity", ADMIN, snakeOrgId + mapOf("dataplane_group_id" to id, "amount" to 1.0), notInOss),
        Route("/api/v1/dataplane_group/create", ORG_ADMIN, snakeOrgId + mapOf("name" to "test", "enabled" to true)),
        Route("/api/v1/dataplane_group/update", ORG_ADMIN, mapOf("dataplane_group_id" to id)),
        Route("/api/v1/dataplane_group/delete", ORG_ADMIN, mapOf("dataplane_group_id" to id)),
        Route("/api/v1/dataplane_group/list", AuthRoleConstants.AUTHENTICATED_USER, snakeOrgId),
        Route("/api/v1/dataplanes/create", ORG_ADMIN, mapOf("dataplane_group_id" to id, "name" to "test", "enabled" to true)),
        Route("/api/v1/dataplanes/update", ORG_ADMIN, mapOf("dataplane_id" to id)),
        Route("/api/v1/dataplanes/delete", ORG_ADMIN, mapOf("dataplane_id" to id)),
        Route("/api/v1/dataplanes/list", ORG_ADMIN, mapOf("dataplane_group_id" to id)),
        Route("/api/v1/dataplanes/token", AuthRoleConstants.AUTHENTICATED_USER, mapOf("client_id" to "client", "client_secret" to "secret")),
        Route("/api/v1/dataplanes/initialize", AuthRoleConstants.AUTHENTICATED_USER, mapOf("client_id" to "client")),
        Route("/api/v1/dataplanes/heartbeat", AuthRoleConstants.AUTHENTICATED_USER, mapOf("client_id" to "client")),
        Route("/api/v1/dataplanes/health", ORG_ADMIN, orgId),
        Route(
          "/api/v1/entitlements/is_entitled",
          AuthRoleConstants.ORGANIZATION_MEMBER,
          snakeOrgId + ("feature_id" to "feature-self-managed-regions"),
        ),
        Route("/api/v1/entitlements/get_entitlements", AuthRoleConstants.AUTHENTICATED_USER, snakeOrgId),
        Route("/api/v1/entitlements/list_plans", ADMIN, emptyMap(), notInOss),
        Route("/api/v1/entitlements/update_plan", ADMIN, snakeOrgId + ("plan_name" to "STANDARD"), notInOss),
        Route("/api/v1/organization_payment_config/$id", ADMIN, emptyMap(), isGet = true),
        Route("/api/v1/organization_payment_config/$id/end_grace_period", ADMIN, emptyMap()),
        Route("/api/v1/organization_payment_config", ADMIN, orgId + ("paymentStatus" to "okay")),
        Route(
          "/api/v1/private_link/create",
          AuthRoleConstants.WORKSPACE_EDITOR,
          mapOf("workspaceId" to id, "name" to "test", "serviceConfig" to mapOf("type" to "endpoint")),
          notInOss,
        ),
        Route("/api/v1/private_link/list", AuthRoleConstants.WORKSPACE_READER, mapOf("workspaceId" to id), notInOss),
        Route("/api/v1/private_link/delete", AuthRoleConstants.WORKSPACE_EDITOR, mapOf("privateLinkId" to id, "workspaceId" to id), notInOss),
        Route("/api/v1/private_link/get", ADMIN, mapOf("privateLinkId" to id), notInOss),
        Route("/api/v1/private_link/update", ADMIN, mapOf("privateLinkId" to id), notInOss),
        Route("/api/v1/private_link/list_by_status", ADMIN, mapOf("status" to "available"), notInOss),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun securedRoutes() = routes.filter { it.secured }

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.body.isNotEmpty() }

    // Dataplane bodies are required in the spec, so a missing body fails before the controller runs.
    @JvmStatic
    fun implementedRoutesWithBody() =
      routes.filter { it.body.isNotEmpty() && !it.isGet && it.allowedStatus != notInOss && !it.path.startsWith("/api/v1/dataplane") }
  }
}

private fun dataplane() =
  Dataplane().apply {
    id = UUID.randomUUID()
    dataplaneGroupId = UUID.randomUUID()
    name = "test"
    enabled = true
    createdAt = 0L
    updatedAt = 0L
  }

private fun dataplaneGroup() =
  DataplaneGroup().apply {
    id = UUID.randomUUID()
    organizationId = UUID.randomUUID()
    name = "test"
    enabled = true
    createdAt = 0L
    updatedAt = 0L
  }

@Requires(property = "spec.name", value = SPEC)
@Singleton
class BillingAndDataplaneApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("billing-and-dataplane-apis-security-test-user", roles))
  }
}
