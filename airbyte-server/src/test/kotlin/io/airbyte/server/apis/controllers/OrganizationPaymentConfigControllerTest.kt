package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationPaymentConfigUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.server.services.OrganizationService
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.kotest.assertions.throwables.shouldThrow
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class OrganizationPaymentConfigControllerTest {
  private var organizationService = mockk<OrganizationService>()
  private var organizationPaymentConfigService = mockk<OrganizationPaymentConfigService>()
  private lateinit var controller: OrganizationPaymentConfigController

  @BeforeEach
  fun setup() {
    controller = OrganizationPaymentConfigController(organizationService, organizationPaymentConfigService)
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
  fun `should throw for config not found on update`() {
    val orgId = UUID.randomUUID()
    every { organizationPaymentConfigService.findByOrganizationId(orgId) } returns null
    shouldThrow<ResourceNotFoundProblem> {
      controller.updateOrganizationPaymentConfig(
        OrganizationPaymentConfigUpdateRequestBody()
          .organizationId(
            orgId,
          ).paymentStatus(OrganizationPaymentConfigUpdateRequestBody.PaymentStatusEnum.MANUAL),
      )
    }
  }
}
