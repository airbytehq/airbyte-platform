/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.OrganizationPaymentConfig
import io.airbyte.db.instance.configs.jooq.generated.enums.PaymentStatus
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

@MicronautTest
internal class OrganizationPaymentConfigRepositoryTest : AbstractConfigRepositoryTest() {
  @AfterEach
  fun tearDown() {
    organizationPaymentConfigRepository.deleteAll()
    workspaceRepository.deleteAll()
    organizationRepository.deleteAll()
  }

  @Test
  fun `test db insertion and retrieval`() {
    val organization =
      Organization(
        name = "Airbyte Inc.",
        email = "contact@airbyte.io",
      )
    val persistedOrg = organizationRepository.save(organization)

    val paymentConfig =
      OrganizationPaymentConfig(
        organizationId = persistedOrg.id!!,
      )

    val countBeforeSave = organizationPaymentConfigRepository.count()
    assertEquals(0L, countBeforeSave)

    organizationPaymentConfigRepository.save(paymentConfig)

    val countAfterSave = organizationPaymentConfigRepository.count()
    assertEquals(1L, countAfterSave)

    val persistedPaymentConfig = organizationPaymentConfigRepository.findById(persistedOrg.id).get()
    assertEquals(persistedOrg.id, persistedPaymentConfig.organizationId)
    assertNull(persistedPaymentConfig.paymentProviderId)
    assertEquals(PaymentStatus.uninitialized, persistedPaymentConfig.paymentStatus)
    assertNull(persistedPaymentConfig.gracePeriodEndAt)
    assertNull(persistedPaymentConfig.usageCategoryOverride)
    assertNotNull(persistedPaymentConfig.createdAt)
    assertNotNull(persistedPaymentConfig.updatedAt)
  }

  @Test
  fun `findByPaymentProviderId`() {
    val organization =
      Organization(
        name = "Airbyte Inc.",
        email = "contact@airbyte.io",
      )
    val persistedOrg = organizationRepository.save(organization)

    val paymentConfig =
      OrganizationPaymentConfig(
        organizationId = persistedOrg.id!!,
        paymentProviderId = "stripe123",
      )

    organizationPaymentConfigRepository.save(paymentConfig)

    val foundPaymentConfig = organizationPaymentConfigRepository.findByPaymentProviderId("stripe123")
    assertNotNull(foundPaymentConfig)
    assertEquals(persistedOrg.id, foundPaymentConfig!!.organizationId)
  }
}
