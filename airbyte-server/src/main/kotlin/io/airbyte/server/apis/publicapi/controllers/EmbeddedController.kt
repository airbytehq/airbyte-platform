/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.ConfigTemplateEntitlement
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.auth.TokenType
import io.airbyte.domain.models.OrganizationId
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.publicApi.server.generated.apis.EmbeddedWidgetApi
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationListItem
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationsList
import io.airbyte.publicApi.server.generated.models.EmbeddedScopedTokenRequest
import io.airbyte.publicApi.server.generated.models.PermissionType
import io.airbyte.server.auth.TokenScopeClaim
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.ws.rs.core.Response
import java.time.Clock
import java.time.temporal.ChronoUnit

// Airbyte Embedded and token generation requires auth,
// so if auth isn't enabled, then this controller is not available.
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Controller
class EmbeddedController(
  val jwtTokenGenerator: JwtTokenGenerator,
  val airbyteAuthConfig: AirbyteAuthConfig,
  val currentUserService: CurrentUserService,
  private val organizationsHandler: OrganizationsHandler,
  val permissionHandler: PermissionHandler,
  val entitlementService: EntitlementService,
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

    // Early return optimization for Instance Admins: they have access to all organizations,
    // so skip expensive per-organization entitlement checks to avoid N+1 performance issues.
    // Instance Admins have unrestricted access by definition, so we return all organizations
    // without filtering by entitlements.
    if (permissionHandler.isUserInstanceAdmin(currentUserId)) {
      val organizationItems =
        organizations.organizations.map { organization ->
          EmbeddedOrganizationListItem(
            organizationId = organization.organizationId,
            organizationName = organization.organizationName,
            permission = PermissionType.INSTANCE_ADMIN,
          )
        }

      return EmbeddedOrganizationsList(
        organizations = organizationItems,
      ).ok()
    }

    // First filter organizations by entitlement
    val entitledOrganizations =
      organizations.organizations.filter { organization ->
        val organizationId = OrganizationId(organization.organizationId)
        val entitled =
          entitlementService.checkEntitlement(
            organizationId,
            ConfigTemplateEntitlement,
          )
        entitled.isEntitled
      }

    // Map to organizations where the user has any permission
    val organizationItems =
      entitledOrganizations
        .mapNotNull { organization ->
          val organizationId = OrganizationId(organization.organizationId)
          val permissionForOrg =
            permissions.find {
              it.organizationId != null &&
                OrganizationId(it.organizationId) == organizationId
            }

          // Only create an item if a permission exists for this organization
          if (permissionForOrg != null) {
            EmbeddedOrganizationListItem(
              organizationId = organizationId.value,
              organizationName = organization.organizationName,
              permission = PermissionType.valueOf(permissionForOrg.permissionType.name),
            )
          } else {
            null
          }
        }

    return EmbeddedOrganizationsList(
      organizations = organizationItems,
    ).ok()
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun generateEmbeddedScopedToken(embeddedScopedTokenRequest: EmbeddedScopedTokenRequest): Response {
    val currentUser = currentUserService.getCurrentUser()
    val workspaceId = embeddedScopedTokenRequest.workspaceId.toString()
    return mapOf("token" to generateToken(workspaceId, currentUser.authUserId, workspaceId))
      .ok()
  }

  private fun generateToken(
    workspaceId: String,
    currentUserId: String,
    externalUserId: String,
  ): String =
    jwtTokenGenerator
      .generateToken(
        mapOf(
          "iss" to airbyteAuthConfig.tokenIssuer,
          "aud" to "airbyte-server",
          "sub" to currentUserId,
          TokenType.EMBEDDED_V1.toClaim(),
          "act" to mapOf("sub" to externalUserId),
          TokenScopeClaim.CLAIM_ID to TokenScopeClaim(workspaceId),
          "roles" to listOf(AuthRoleConstants.EMBEDDED_END_USER),
          "exp" to clock.instant().plus(airbyteAuthConfig.tokenExpiration.embeddedTokenExpirationInMinutes, ChronoUnit.MINUTES).epochSecond,
        ),
      ).orElseThrow {
        IllegalStateException("Could not generate token")
      }
}

private fun <T> T.ok() = Response.status(Response.Status.OK.statusCode).entity(this).build()
