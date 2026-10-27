/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SsoConfigApi
import io.airbyte.api.server.generated.models.ActivateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.GetSSOConfigRequestBody
import io.airbyte.api.server.generated.models.SSOConfigRead
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.api.server.generated.models.ValidateSSOTokenRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.data.services.impls.data.mappers.toApi
import io.airbyte.data.services.impls.data.mappers.toDomain
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.domain.services.sso.SsoConfigDomainService
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.SsoAdminRule
import io.airbyte.server.auth.SsoDeleteRule
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
open class SSOConfigApiController(
  private val ssoConfigDomainService: SsoConfigDomainService,
  private val entitlementService: EntitlementService,
) : SsoConfigApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SsoAdminRule::class,
  )
  override fun getSsoConfig(getSSOConfigRequestBody: GetSSOConfigRequestBody): SSOConfigRead {
    entitlementService.ensureEntitled(OrganizationId(getSSOConfigRequestBody.organizationId), SsoEntitlement)

    val ssoConfig = ssoConfigDomainService.retrieveSsoConfig(getSSOConfigRequestBody.organizationId)
    return SSOConfigRead(
      organizationId = getSSOConfigRequestBody.organizationId,
      companyIdentifier = ssoConfig.companyIdentifier,
      clientId = ssoConfig.clientId,
      clientSecret = ssoConfig.clientSecret,
      emailDomains = ssoConfig.emailDomains,
      status = ssoConfig.status.toApi(),
      defaultRole = ssoConfig.defaultRole.toApi(),
    )
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SSO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SsoAdminRule::class,
  )
  override fun createSsoConfig(createSSOConfigRequestBody: CreateSSOConfigRequestBody) {
    entitlementService.ensureEntitled(OrganizationId(createSSOConfigRequestBody.organizationId), SsoEntitlement)

    execute<Any?> {
      ssoConfigDomainService.createAndStoreSsoConfig(
        SsoConfig(
          createSSOConfigRequestBody.organizationId,
          createSSOConfigRequestBody.companyIdentifier,
          createSSOConfigRequestBody.clientId,
          createSSOConfigRequestBody.clientSecret,
          createSSOConfigRequestBody.discoveryUrl,
          createSSOConfigRequestBody.emailDomain,
          createSSOConfigRequestBody.status.toDomain(),
          // Pass through null when the caller omits the role so the domain layer can tell
          // "unspecified" (leave a stored role unchanged on update) from an explicit choice.
          createSSOConfigRequestBody.defaultRole?.toDomain(),
        ),
      )
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SSO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SsoDeleteRule::class,
  )
  override fun deleteSsoConfig(deleteSSOConfigRequestBody: DeleteSSOConfigRequestBody) {
    entitlementService.ensureEntitled(OrganizationId(deleteSSOConfigRequestBody.organizationId), SsoEntitlement)

    execute<Any?> {
      ssoConfigDomainService.deleteSsoConfig(
        deleteSSOConfigRequestBody.organizationId,
        deleteSSOConfigRequestBody.companyIdentifier,
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SSO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SsoAdminRule::class,
  )
  override fun updateSsoCredentials(updateSSOCredentialsRequestBody: UpdateSSOCredentialsRequestBody) {
    entitlementService.ensureEntitled(OrganizationId(updateSSOCredentialsRequestBody.organizationId), SsoEntitlement)

    execute<Any?> {
      ssoConfigDomainService.updateClientCredentials(
        SsoKeycloakIdpCredentials(
          organizationId = updateSSOCredentialsRequestBody.organizationId,
          clientId = updateSSOCredentialsRequestBody.clientId,
          clientSecret = updateSSOCredentialsRequestBody.clientSecret,
        ),
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SSO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SsoAdminRule::class,
  )
  override fun activateSsoConfig(activateSSOConfigRequestBody: ActivateSSOConfigRequestBody) {
    entitlementService.ensureEntitled(OrganizationId(activateSSOConfigRequestBody.organizationId), SsoEntitlement)

    execute<Any?> {
      ssoConfigDomainService.activateSsoConfig(
        activateSSOConfigRequestBody.organizationId,
        activateSSOConfigRequestBody.emailDomain,
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.SSO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SsoAdminRule::class,
  )
  override fun validateSsoToken(validateSSOTokenRequestBody: ValidateSSOTokenRequestBody) {
    entitlementService.ensureEntitled(OrganizationId(validateSSOTokenRequestBody.organizationId), SsoEntitlement)

    execute<Any?> {
      ssoConfigDomainService.validateToken(
        validateSSOTokenRequestBody.organizationId,
        validateSSOTokenRequestBody.accessToken,
      )
    }
  }
}
