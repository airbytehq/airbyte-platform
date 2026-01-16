/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.SSOTokenValidationProblem
import io.airbyte.api.server.generated.models.ActivateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.CreateSSOConfigRequestBody
import io.airbyte.api.server.generated.models.DeleteSSOConfigRequestBody
import io.airbyte.api.server.generated.models.GetSSOConfigRequestBody
import io.airbyte.api.server.generated.models.SSOConfigStatus
import io.airbyte.api.server.generated.models.UpdateSSOCredentialsRequestBody
import io.airbyte.api.server.generated.models.ValidateSSOTokenRequestBody
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
  fun `activateSsoConfig activates a draft config with emailDomain`() {
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
  fun `activateSsoConfig activates a draft config without emailDomain`() {
    val orgId = OrganizationId(UUID.randomUUID())
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.activateSsoConfig(orgId.value, null) } just Runs

    ssoConfigController.activateSsoConfig(
      ActivateSSOConfigRequestBody(
        organizationId = orgId.value,
        emailDomain = null,
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.activateSsoConfig(orgId.value, null) }
  }

  @Test
  fun `validateSsoToken succeeds with 204 when token is valid`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val accessToken = "valid-token"
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.validateToken(orgId.value, accessToken) } just Runs

    // Should not throw an exception
    ssoConfigController.validateSsoToken(
      ValidateSSOTokenRequestBody(
        organizationId = orgId.value,
        accessToken = accessToken,
      ),
    )

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.validateToken(orgId.value, accessToken) }
  }

  @Test
  fun `validateSsoToken throws SSOTokenValidationProblem when token is invalid`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val accessToken = "invalid-token"
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.validateToken(orgId.value, accessToken) } throws
      SSOTokenValidationProblem(
        io.airbyte.api.problems.model.generated
          .ProblemSSOTokenValidationData()
          .organizationId(orgId.value)
          .errorMessage("Token is invalid or expired"),
      )

    val exception =
      assertThrows<SSOTokenValidationProblem> {
        ssoConfigController.validateSsoToken(
          ValidateSSOTokenRequestBody(
            organizationId = orgId.value,
            accessToken = accessToken,
          ),
        )
      }

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.validateToken(orgId.value, accessToken) }
    assertEquals(401, exception.problem.getStatus())
    assertEquals(
      "Token is invalid or expired",
      (exception.problem.getData() as io.airbyte.api.problems.model.generated.ProblemSSOTokenValidationData).errorMessage,
    )
  }

  @Test
  fun `validateSsoToken throws SSOTokenValidationProblem when validation fails with exception`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val accessToken = "error-token"
    val errorMessage = "Token validation failed: Connection refused"
    every { entitlementService.ensureEntitled(orgId, SsoEntitlement) } just Runs
    every { ssoConfigDomainService.validateToken(orgId.value, accessToken) } throws
      SSOTokenValidationProblem(
        io.airbyte.api.problems.model.generated
          .ProblemSSOTokenValidationData()
          .organizationId(orgId.value)
          .errorMessage(errorMessage),
      )

    val exception =
      assertThrows<SSOTokenValidationProblem> {
        ssoConfigController.validateSsoToken(
          ValidateSSOTokenRequestBody(
            organizationId = orgId.value,
            accessToken = accessToken,
          ),
        )
      }

    verify(exactly = 1) { entitlementService.ensureEntitled(orgId, SsoEntitlement) }
    verify(exactly = 1) { ssoConfigDomainService.validateToken(orgId.value, accessToken) }
    assertEquals(401, exception.problem.getStatus())
    assertEquals(errorMessage, (exception.problem.getData() as io.airbyte.api.problems.model.generated.ProblemSSOTokenValidationData).errorMessage)
  }
}
