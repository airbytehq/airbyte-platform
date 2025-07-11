/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.GetSSOConfigRequestBody
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SsoConfigUpdateEntitlement
import io.airbyte.domain.models.SsoConfigRetrieval
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
    val orgId = UUID.randomUUID()
    every { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) } just Runs
    every { ssoConfigDomainService.retrieveSsoConfig(orgId) } returns
      SsoConfigRetrieval(
        companyIdentifier = "id",
        clientId = "client-id",
        clientSecret = "client-secret",
        emailDomains = listOf("domain"),
      )

    val result =
      ssoConfigController.getSsoConfig(
        GetSSOConfigRequestBody(orgId),
      )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) }

    assert(result.organizationId == orgId)
    assert(result.companyIdentifier == "id")
    assert(result.clientId == "client-id")
    assert(result.clientSecret == "client-secret")
    assert(result.emailDomains == listOf("domain"))
  }

  @Test
  fun `createSsoConfig creates a new config`() {
    val orgId = UUID.randomUUID()
    every { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) } just Runs
    every { ssoConfigDomainService.createAndStoreSsoConfig(any()) } just Runs

    ssoConfigController.createSsoConfig(
      CreateSSOConfigRequestBody(
        organizationId = orgId,
        companyIdentifier = "id",
        clientId = "client-id",
        clientSecret = "client-secret",
        discoveryUrl = "https://www.airbyte.io",
        emailDomain = "domain",
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.createAndStoreSsoConfig(any()) }
  }

  @Test
  fun `deleteSsoConfig removes the existing config`() {
    val orgId = UUID.randomUUID()
    every { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) } just Runs
    every { ssoConfigDomainService.deleteSsoConfig(orgId, any()) } just Runs

    ssoConfigController.deleteSsoConfig(
      DeleteSSOConfigRequestBody(
        organizationId = orgId,
        companyIdentifier = "id",
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.deleteSsoConfig(orgId, any()) }
  }

  @Test
  fun `updateSsoConfig updates a new config`() {
    val orgId = UUID.randomUUID()
    every { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) } just Runs
    every { ssoConfigDomainService.updateClientCredentials(any()) } just Runs

    ssoConfigController.updateSsoCredentials(
      UpdateSSOCredentialsRequestBody(
        organizationId = orgId,
        clientId = "client-id",
        clientSecret = "client-secret",
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoConfigUpdateEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.updateClientCredentials(any()) }
  }
}
