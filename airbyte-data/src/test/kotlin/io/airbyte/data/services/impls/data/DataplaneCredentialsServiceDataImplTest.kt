/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.helpers.DataplanePasswordEncoder
import io.airbyte.data.repositories.DataplaneClientCredentialsRepository
import io.airbyte.data.repositories.entities.DataplaneClientCredentials
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class DataplaneCredentialsServiceDataImplTest {
  companion object {
    val dataplaneClientCredentialsRepository = mockk<DataplaneClientCredentialsRepository>()
    val dataplanePasswordEncoder = mockk<DataplanePasswordEncoder>()
    val service =
      DataplaneCredentialsServiceDataImpl(
        dataplaneClientCredentialsRepository,
        dataplanePasswordEncoder,
      )
  }

  @Test
  fun `should create credentials`() {
    val dataplaneId = UUID.randomUUID()
    val entity =
      DataplaneClientCredentials(
        id = UUID.randomUUID(),
        dataplaneId = dataplaneId,
        clientId = "test-client-id",
        clientSecret = "test-client-secret",
      )

    every { dataplaneClientCredentialsRepository.save(any()) } returns entity
    every { dataplanePasswordEncoder.encode(any()) } returns "password"

    val result = service.createCredentials(dataplaneId)

    result.dataplaneId shouldBe dataplaneId
    result.clientId shouldBe "test-client-id"
    result.clientSecret.shouldNotBe(null)
  }

  @Test
  fun `should delete credentials`() {
    val credentialsId = UUID.randomUUID()
    val entity =
      DataplaneClientCredentials(
        id = credentialsId,
        dataplaneId = UUID.randomUUID(),
        clientId = "test-client-id",
        clientSecret = "test-client-secret",
      )

    every { dataplaneClientCredentialsRepository.findById(credentialsId) } returns Optional.of(entity)
    every { dataplaneClientCredentialsRepository.delete(entity) } just Runs

    val result = service.deleteCredentials(credentialsId)

    result.id shouldBe credentialsId
  }

  @Test
  fun `should throw when deleting nonexistent credentials`() {
    val credentialsId = UUID.randomUUID()

    every { dataplaneClientCredentialsRepository.findById(credentialsId) } returns Optional.empty()

    assertThrows<IllegalArgumentException> {
      service.deleteCredentials(credentialsId)
    }
  }

  @Test
  fun `getDataplaneId should return dataplane id associated with provided client id`() {
    val clientId = UUID.randomUUID().toString()
    val dataplaneId = UUID.randomUUID()
    val creds =
      DataplaneClientCredentials(
        id = UUID.randomUUID(),
        dataplaneId = dataplaneId,
        clientId = clientId,
        clientSecret = "secret",
      )

    every { dataplaneClientCredentialsRepository.findByClientId(clientId) } returns creds

    val result = service.getDataplaneId(clientId)
    Assertions.assertEquals(dataplaneId, result)
  }

  @Test
  fun `getDataplaneId should should throw when credentials not found for client id`() {
    val clientId = UUID.randomUUID().toString()

    every { dataplaneClientCredentialsRepository.findByClientId(clientId) } returns null

    assertThrows<IllegalArgumentException> {
      service.getDataplaneId(clientId)
    }
  }
}
