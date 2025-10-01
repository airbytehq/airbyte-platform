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
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.data.services.impls.data.mappers.toApi
import io.airbyte.data.services.impls.data.mappers.toDomain
import io.airbyte.domain.models.OrganizationId
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
    entitlementService.ensureEntitled(OrganizationId(getSSOConfigRequestBody.organizationId), SsoEntitlement)

    val ssoConfig = ssoConfigDomainService.retrieveSsoConfig(getSSOConfigRequestBody.organizationId)
    return SSOConfigRead(
      organizationId = getSSOConfigRequestBody.organizationId,
      companyIdentifier = ssoConfig.companyIdentifier,
      clientId = ssoConfig.clientId,
      clientSecret = ssoConfig.clientSecret,
      emailDomains = ssoConfig.emailDomains,
      status = ssoConfig.status.toApi(),
    )
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
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
        ),
      )
      null
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun deleteSsoConfig(deleteSSOConfigRequestBody: DeleteSSOConfigRequestBody) {
    entitlementService.ensureEntitled(OrganizationId(deleteSSOConfigRequestBody.organizationId), SsoEntitlement)

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
}
