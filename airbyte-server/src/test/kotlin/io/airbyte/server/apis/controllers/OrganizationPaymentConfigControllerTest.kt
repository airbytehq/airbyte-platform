package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationPaymentConfigRead
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.OrganizationService
import io.kotest.assertions.throwables.shouldThrow
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class OrganizationPaymentConfigControllerTest {
  private var organizationService = mockk<OrganizationService>()
  private var organizationPaymentConfigService = mockk<OrganizationPaymentConfigService>()
  private lateinit var controller: OrganizationPaymentConfigController

  @BeforeEach
  fun setup() {
    controller = OrganizationPaymentConfigController(organizationPaymentConfigService, organizationService)
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
    every { organizationService.getOrganization(orgId) } returns Optional.empty()
    shouldThrow<ResourceNotFoundProblem> {
      controller.updateOrganizationPaymentConfig(
        OrganizationPaymentConfigRead().organizationId(orgId).paymentStatus(OrganizationPaymentConfigRead.PaymentStatusEnum.MANUAL),
      )
    }
  }
}
