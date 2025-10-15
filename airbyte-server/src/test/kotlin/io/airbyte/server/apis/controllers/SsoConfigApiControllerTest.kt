/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.model.generated.ProblemSSOTokenValidationData
import io.airbyte.api.problems.throwable.generated.SSOTokenValidationProblem
import io.airbyte.api.server.generated.models.ActivateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.ExchangeSSOAuthCodeRequestBody
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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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

  @Test
  fun `activateSsoConfig activates a draft config`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val emailDomain = "airbyte.com"
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.activateSsoConfig(orgId.value, emailDomain) } just Runs

    ssoConfigController.activateSsoConfig(
      ActivateSSOConfigRequestBody(
        organizationId = orgId.value,
        emailDomain = emailDomain,
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.activateSsoConfig(orgId.value, emailDomain) }
  }

  @Test
  fun `exchangeSsoAuthCode successfully returns access token`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val authCode = "auth-code-123"
    val codeVerifier = "verifier-xyz"
    val redirectUri = "https://cloud.airbyte.com/callback"
    val accessToken = "access-token-abc"

    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.exchangeAuthCodeAndValidate(orgId.value, authCode, codeVerifier, redirectUri) } returns accessToken

    val result =
      ssoConfigController.exchangeSsoAuthCode(
        ExchangeSSOAuthCodeRequestBody(
          organizationId = orgId.value,
          authorizationCode = authCode,
          codeVerifier = codeVerifier,
          redirectUri = redirectUri,
        ),
      )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.exchangeAuthCodeAndValidate(orgId.value, authCode, codeVerifier, redirectUri) }
    assertEquals(accessToken, result.accessToken)
  }

  @Test
  fun `exchangeSsoAuthCode throws SSOTokenValidationProblem when exchange fails`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val authCode = "invalid-code"
    val codeVerifier = "verifier-xyz"
    val redirectUri = "https://cloud.airbyte.com/callback"

    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.exchangeAuthCodeAndValidate(orgId.value, authCode, codeVerifier, redirectUri) } throws
      SSOTokenValidationProblem(
        ProblemSSOTokenValidationData()
          .organizationId(orgId.value)
          .errorMessage("Failed to exchange authorization code: Code not valid"),
      )

    val exception =
      assertThrows<SSOTokenValidationProblem> {
        ssoConfigController.exchangeSsoAuthCode(
          ExchangeSSOAuthCodeRequestBody(
            organizationId = orgId.value,
            authorizationCode = authCode,
            codeVerifier = codeVerifier,
            redirectUri = redirectUri,
          ),
        )
      }

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.exchangeAuthCodeAndValidate(orgId.value, authCode, codeVerifier, redirectUri) }
    assertEquals(401, exception.problem.getStatus())
    assertEquals(
      "Failed to exchange authorization code: Code not valid",
      (exception.problem.getData() as ProblemSSOTokenValidationData).errorMessage,
    )
  }
}
