/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SsoConfigApi
import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.GetSSOConfigRequestBody
import io.airbyte.api.server.generated.models.SSOConfigRead
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SsoConfigUpdateEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.domain.services.sso.SsoConfigDomainService
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller
open class SSOConfigApiController(
  private val ssoConfigDomainService: SsoConfigDomainService,
  private val entitlementService: EntitlementService,
) : SsoConfigApi {
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getSsoConfig(getSSOConfigRequestBody: GetSSOConfigRequestBody): SSOConfigRead {
    entitlementService.ensureEntitled(getSSOConfigRequestBody.organizationId, SsoConfigUpdateEntitlement)

    val ssoConfig = ssoConfigDomainService.retrieveSsoConfig(getSSOConfigRequestBody.organizationId)
    return SSOConfigRead(
      organizationId = getSSOConfigRequestBody.organizationId,
      companyIdentifier = ssoConfig.companyIdentifier,
      clientId = ssoConfig.clientId,
      clientSecret = ssoConfig.clientSecret,
      emailDomains = ssoConfig.emailDomains,
    )
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createSsoConfig(createSSOConfigRequestBody: CreateSSOConfigRequestBody) {
    entitlementService.ensureEntitled(createSSOConfigRequestBody.organizationId, SsoConfigUpdateEntitlement)

    execute<Any?> {
      ssoConfigDomainService.createAndStoreSsoConfig(
        SsoConfig(
          createSSOConfigRequestBody.organizationId,
          createSSOConfigRequestBody.companyIdentifier,
          createSSOConfigRequestBody.clientId,
          createSSOConfigRequestBody.clientSecret,
          createSSOConfigRequestBody.discoveryUrl,
          createSSOConfigRequestBody.emailDomain,
        ),
      )
      null
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteSsoConfig(deleteSSOConfigRequestBody: DeleteSSOConfigRequestBody) {
    entitlementService.ensureEntitled(deleteSSOConfigRequestBody.organizationId, SsoConfigUpdateEntitlement)

    execute<Any?> {
      ssoConfigDomainService.deleteSsoConfig(
        deleteSSOConfigRequestBody.organizationId,
        deleteSSOConfigRequestBody.companyIdentifier,
      )
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateSsoCredentials(updateSSOCredentialsRequestBody: UpdateSSOCredentialsRequestBody) {
    entitlementService.ensureEntitled(updateSSOCredentialsRequestBody.organizationId, SsoConfigUpdateEntitlement)

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
}
