/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SsoConfigApi
import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.SsoConfig
import io.airbyte.domain.models.SsoKeycloakIdpCredentials
import io.airbyte.domain.services.sso.SsoConfigDomainService
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import java.util.UUID

@Controller
open class SSOConfigApiController(
  private val ssoConfigDomainService: SsoConfigDomainService,
) : SsoConfigApi {
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun createSsoConfig(createSSOConfigRequestBody: CreateSSOConfigRequestBody) {
    execute<Any?> {
      ssoConfigDomainService.createAndStoreSsoConfig(
        SsoConfig(
          UUID.fromString(createSSOConfigRequestBody.organizationId),
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
    execute<Any?> {
      ssoConfigDomainService.deleteSsoConfig(
        UUID.fromString(deleteSSOConfigRequestBody.organizationId),
        deleteSSOConfigRequestBody.companyIdentifier,
      )
    }
  }

  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  override fun updateSsoCredentials(updateSSOCredentialsRequestBody: UpdateSSOCredentialsRequestBody) {
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
