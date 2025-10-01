/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.GetSSOConfigRequestBody
import io.airbyte.api.server.generated.models.SSOConfigStatus
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SsoConfigRetrieval
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.domain.services.sso.SsoConfigDomainService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class SsoConfigApiControllerTest {
  companion object {
    private val ssoConfigDomainService = mockk<SsoConfigDomainService>()
    private val entitlementService = mockk<EntitlementService>()
    private val ssoConfigController =
      SSOConfigApiController(
        ssoConfigDomainService,
        entitlementService,
      )
  }

  @Test
  fun `getSsoConfig returns the config`() {
    val orgId = OrganizationId(UUID.randomUUID())
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.retrieveSsoConfig(orgId.value) } returns
      SsoConfigRetrieval(
        companyIdentifier = "id",
        clientId = "client-id",
        clientSecret = "client-secret",
        emailDomains = listOf("domain"),
        status = SsoConfigStatus.ACTIVE,
      )

    val result =
      ssoConfigController.getSsoConfig(
        GetSSOConfigRequestBody(orgId.value),
      )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }

    assert(result.organizationId == orgId.value)
    assert(result.companyIdentifier == "id")
    assert(result.clientId == "client-id")
    assert(result.clientSecret == "client-secret")
    assert(result.emailDomains == listOf("domain"))
  }

  @Test
  fun `createSsoConfig creates a new config`() {
    val orgId = OrganizationId(UUID.randomUUID())
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.createAndStoreSsoConfig(any()) } just Runs

    ssoConfigController.createSsoConfig(
      CreateSSOConfigRequestBody(
        organizationId = orgId.value,
        companyIdentifier = "id",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://www.airbyte.io",
        emailDomain = "domain",
        status = SSOConfigStatus.ACTIVE,
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.createAndStoreSsoConfig(any()) }
  }

  @Test
  fun `deleteSsoConfig removes the existing config`() {
    val orgId = OrganizationId(UUID.randomUUID())
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.deleteSsoConfig(orgId.value, any()) } just Runs

    ssoConfigController.deleteSsoConfig(
      DeleteSSOConfigRequestBody(
        organizationId = orgId.value,
        companyIdentifier = "id",
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.deleteSsoConfig(orgId.value, any()) }
  }

  @Test
  fun `updateSsoConfig updates a new config`() {
    val orgId = OrganizationId(UUID.randomUUID())
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.updateClientCredentials(any()) } just Runs

    ssoConfigController.updateSsoCredentials(
      UpdateSSOCredentialsRequestBody(
        organizationId = orgId.value,
        clientId = "client-id",
        clientSecret = "client-secret",
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.updateClientCredentials(any()) }
  }
}
