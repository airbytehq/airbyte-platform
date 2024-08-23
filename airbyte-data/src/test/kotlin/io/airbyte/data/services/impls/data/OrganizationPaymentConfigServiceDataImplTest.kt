package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.OrganizationPaymentConfigRepository
import io.airbyte.data.repositories.entities.OrganizationPaymentConfig
import io.airbyte.db.instance.configs.jooq.generated.enums.PaymentStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.UsageCategoryOverride
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class OrganizationPaymentConfigServiceDataImplTest {
  private val organizationPaymentConfigRepository = mockk<OrganizationPaymentConfigRepository>()
  private val organizationPaymentConfigService = OrganizationPaymentConfigServiceDataImpl(organizationPaymentConfigRepository)

  private lateinit var testOrganizationId: UUID

  @BeforeEach
  fun setup() {
    clearAllMocks()
    testOrganizationId = UUID.randomUUID()
    val orgPaymentConfig =
      OrganizationPaymentConfig(
        organizationId = testOrganizationId,
        paymentProviderId = "provider-id",
        paymentStatus = PaymentStatus.grace_period,
        gracePeriodEndAt = OffsetDateTime.now().plusDays(30),
        usageCategoryOverride = UsageCategoryOverride.internal,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
    every { organizationPaymentConfigRepository.findById(testOrganizationId) } returns Optional.of(orgPaymentConfig)
    every { organizationPaymentConfigRepository.findById(not(testOrganizationId)) } returns Optional.empty()
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
  }

  @Test
  fun `test find by organization id`() {
    val result = organizationPaymentConfigService.findByOrganizationId(testOrganizationId)
    assertNotNull(result)
    assertEquals(testOrganizationId, result?.organizationId)
    assertEquals("provider-id", result?.paymentProviderId)
    verify { organizationPaymentConfigRepository.findById(testOrganizationId) }
  }

  @Test
  fun `test find by non-existent organization id`() {
    val nonExistentId = UUID.randomUUID()
    val result = organizationPaymentConfigService.findByOrganizationId(nonExistentId)
    assertNull(result)
    verify { organizationPaymentConfigRepository.findById(nonExistentId) }
  }
}
