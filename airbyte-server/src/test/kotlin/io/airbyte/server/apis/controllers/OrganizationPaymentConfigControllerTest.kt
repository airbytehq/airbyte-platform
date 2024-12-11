package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationPaymentConfigRead
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.server.services.OrganizationService
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.kotest.assertions.throwables.shouldThrow
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.OrganizationService as OrganizationRepository

class OrganizationPaymentConfigControllerTest {
  private var organizationService = mockk<OrganizationService>()
  private var organizationRepository = mockk<OrganizationRepository>()
  private var organizationPaymentConfigService = mockk<OrganizationPaymentConfigService>()
  private lateinit var controller: OrganizationPaymentConfigController

  @BeforeEach
  fun setup() {
    controller = OrganizationPaymentConfigController(organizationService, organizationPaymentConfigService, organizationRepository)
  }

  @Test
  fun `should throw for config not found on delete`() {
    val orgId = UUID.randomUUID()
    every { organizationPaymentConfigService.findByOrganizationId(orgId) } returns null
    shouldThrow<ResourceNotFoundProblem> {
      controller.deleteOrganizationPaymentConfig(orgId)
    }
  }

  @Test
  fun `should throw for config not found on get`() {
    val orgId = UUID.randomUUID()
    every { organizationPaymentConfigService.findByOrganizationId(orgId) } returns null
    shouldThrow<ResourceNotFoundProblem> {
      controller.getOrganizationPaymentConfig(orgId)
    }
  }

  @Test
  fun `invalid organization id should fail saving payment config`() {
    val orgId = UUID.randomUUID()
    every { organizationRepository.getOrganization(orgId) } returns Optional.empty()
    shouldThrow<ResourceNotFoundProblem> {
      controller.updateOrganizationPaymentConfig(
        OrganizationPaymentConfigRead().organizationId(orgId).paymentStatus(OrganizationPaymentConfigRead.PaymentStatusEnum.MANUAL),
      )
    }
  }
}
