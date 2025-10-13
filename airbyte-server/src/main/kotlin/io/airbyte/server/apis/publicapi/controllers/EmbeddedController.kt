/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.EmbeddedWidgetApi
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationListItem
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationsList
import io.airbyte.publicApi.server.generated.models.PermissionType
import io.micronaut.context.annotation.Requires
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response

// Airbyte Embedded and token generation requires auth,
// so if auth isn't enabled, then this controller is not available.
@Requires(property = "micronaut.security.enabled", value = "true")
@Requires(property = "micronaut.security.token.jwt.enabled", value = "true")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Controller
class EmbeddedController(
  val currentUserService: CurrentUserService,
  private val organizationsHandler: OrganizationsHandler,
  val permissionHandler: PermissionHandler,
  val licenseEntitlementChecker: LicenseEntitlementChecker,
) : EmbeddedWidgetApi {
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
}

private fun <T> T.ok() = Response.status(Response.Status.OK.statusCode).entity(this).build()
