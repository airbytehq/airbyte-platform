/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.OrganizationPaymentConfigRepository
import io.airbyte.data.repositories.entities.OrganizationPaymentConfig
import io.airbyte.db.instance.configs.jooq.generated.enums.PaymentStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.UsageCategoryOverride
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class OrganizationPaymentConfigServiceDataImplTest {
  private val organizationPaymentConfigRepository = mockk<OrganizationPaymentConfigRepository>()
  private val organizationPaymentConfigService = OrganizationPaymentConfigServiceDataImpl(organizationPaymentConfigRepository)

  private val testOrganizationId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    clearAllMocks()
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
    result.shouldNotBeNull()
    result.organizationId shouldBe testOrganizationId
    result.paymentProviderId shouldBe "provider-id"
    verify { organizationPaymentConfigRepository.findById(testOrganizationId) }
  }

  @Test
  fun `test find by non-existent organization id`() {
    val nonExistentId = UUID.randomUUID()
    val result = organizationPaymentConfigService.findByOrganizationId(nonExistentId)
    result.shouldBeNull()
    verify { organizationPaymentConfigRepository.findById(nonExistentId) }
  }
}
