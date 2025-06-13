/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.EmbeddedWorkspacesHandler
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.TokenType
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.EmbeddedWidgetApi
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationListItem
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationsList
import io.airbyte.publicApi.server.generated.models.EmbeddedWidgetRequest
import io.airbyte.publicApi.server.generated.models.PermissionType
import io.airbyte.server.auth.TokenScopeClaim
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.inject.Named
import jakarta.ws.rs.core.Response
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import java.time.Clock
import java.time.temporal.ChronoUnit
import java.util.Base64
import java.util.UUID

// Airbyte Embedded and token generation requires auth,
// so if auth isn't enabled, then this controller is not available.
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Controller
class EmbeddedController(
  val jwtTokenGenerator: JwtTokenGenerator,
  val tokenExpirationConfig: TokenExpirationConfig,
  val currentUserService: CurrentUserService,
  private val organizationsHandler: OrganizationsHandler,
  val permissionHandler: PermissionHandler,
  val embeddedWorkspacesHandler: EmbeddedWorkspacesHandler,
  val licenseEntitlementChecker: LicenseEntitlementChecker,
  @Named("airbyteUrl") val airbyteUrl: String,
  @Property(name = "airbyte.auth.token-issuer") private val tokenIssuer: String,
) : EmbeddedWidgetApi {
  var clock: Clock = Clock.systemUTC()

  override fun listEmbeddedOrganizationsByUser(): Response {
    val currentUserId = currentUserService.getCurrentUser().userId
    val organizations =
      organizationsHandler
        .listOrganizationsByUser(
          ListOrganizationsByUserRequestBody().userId(currentUserId),
        )

    val permissions = permissionHandler.listPermissionsForUser(currentUserId)

    // First filter organizations by entitlement
    val entitledOrganizations =
      organizations.organizations.filter { organization ->
        val organizationId = OrganizationId(organization.organizationId)
        licenseEntitlementChecker.checkEntitlements(
          organizationId.value,
          Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
        )
      }

    // Then map the filtered organizations to the desired output format
    val organizationsListItems =
      entitledOrganizations.map { organization ->
        val organizationId = OrganizationId(organization.organizationId)
        val permissionForOrg = permissions.find { OrganizationId(it.organizationId) == organizationId }

        if (permissionForOrg == null) {
          throw IllegalStateException(
            "No permission found for user $currentUserId in organization ${organization.organizationName} (${organization.organizationId})",
          )
        }

        EmbeddedOrganizationListItem(
          organizationId = organizationId.value,
          organizationName = organization.organizationName,
          permission = PermissionType.valueOf(permissionForOrg.permissionType.name),
        )
      }

    return EmbeddedOrganizationsList(organizations = organizationsListItems).ok()
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getEmbeddedWidget(req: EmbeddedWidgetRequest): Response {
    val organizationId = OrganizationId(req.organizationId)

    licenseEntitlementChecker.ensureEntitled(
      organizationId.value,
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS,
    )

    val currentUser = currentUserService.getCurrentUser()
    val externalUserId = req.externalUserId ?: UUID.randomUUID().toString()

    val workspaceId =
      embeddedWorkspacesHandler.getOrCreate(
        organizationId,
        req.externalUserId,
      )

    val widgetUrl =
      airbyteUrl
        .toHttpUrlOrNull()!!
        .newBuilder()
        .encodedPath("/embedded-widget")
        .addQueryParameter("workspaceId", workspaceId.value.toString())
        .addQueryParameter("allowedOrigin", req.allowedOrigin)
        .toString()

    val data =
      mapOf(
        "token" to generateToken(workspaceId.value.toString(), currentUser.authUserId, externalUserId),
        "widgetUrl" to widgetUrl,
      )
    val json = Jsons.serialize(data)

    // For debugging/dev, it's sometimes easier to just have the raw JSON
    // instead of the base64-encoded string. That's available by passing "?debug" in the URL.
    if (isDebug()) {
      return json.ok()
    }

    // This endpoint is different from most – it returns an "opaque" base64-encoded string,
    // which decodes to a JSON object. This is because there is an intermediate party (Operator)
    // which needs to pass this value to the Embedded widget code.
    // All fields passed to the EmbeddedWidget should be included in the encoded string.
    // We still return a json object in case we ever need to return additional information that should not be encoded.
    val encoded = Base64.getEncoder().encodeToString(json.toByteArray())
    return mapOf("token" to encoded).ok()
  }

  private fun isDebug(): Boolean = ServerRequestContext.currentRequest<Any>().map { it.parameters.contains("debug") }.orElse(false)

  private fun generateToken(
    workspaceId: String,
    currentUserId: String,
    externalUserId: String,
  ): String =
    jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to tokenIssuer,
          "aud" to "airbyte-server",
          "sub" to currentUserId,
          TokenType.EMBEDDED_V1.toClaim(),
          "act" to mapOf("sub" to externalUserId),
          TokenScopeClaim.CLAIM_ID to TokenScopeClaim(workspaceId),
          "roles" to listOf(AuthRoleConstants.EMBEDDED_END_USER),
          "exp" to clock.instant().plus(tokenExpirationConfig.embeddedTokenExpirationInMinutes, ChronoUnit.MINUTES).epochSecond,
        ),
      ).orElseThrow {
        IllegalStateException("Could not generate token")
      }
}

private fun <T> T.ok() = Response.status(Response.Status.OK.statusCode).entity(this).build()
