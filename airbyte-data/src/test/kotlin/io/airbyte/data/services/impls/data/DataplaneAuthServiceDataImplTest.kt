/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.data.helpers.DataplanePasswordEncoder
import io.airbyte.data.repositories.DataplaneClientCredentialsRepository
import io.airbyte.data.repositories.entities.DataplaneClientCredentials
import io.airbyte.data.services.impls.data.DataplaneAuthServiceDataImpl
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.micronaut.context.annotation.Property
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

@Property(name = "micronaut.security.enabled", value = "true")
class DataplaneAuthServiceDataImplTest {
  companion object {
    val dataplaneClientCredentialsRepository = mockk<DataplaneClientCredentialsRepository>()
    val jwtTokenGenerator = mockk<JwtTokenGenerator>()
    val dataplanePasswordEncoder = mockk<DataplanePasswordEncoder>()
    val service =
      DataplaneAuthServiceDataImpl(
        dataplaneClientCredentialsRepository,
        jwtTokenGenerator,
        dataplanePasswordEncoder,
        TokenExpirationConfig(),
      )
  }

  @Test
  fun `should create credentials`() {
    val dataplaneId = UUID.randomUUID()
    val createdById = UUID.randomUUID()
    val entity =
      DataplaneClientCredentials(
        id = UUID.randomUUID(),
        dataplaneId = dataplaneId,
        clientId = "test-client-id",
        clientSecret = "test-client-secret",
        createdBy = createdById,
      )

    every { dataplaneClientCredentialsRepository.save(any()) } returns entity
    every { dataplanePasswordEncoder.encode(any()) } returns "password"

    val result = service.createCredentials(dataplaneId, createdById)

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
        createdBy = UUID.randomUUID(),
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
  fun `should get token`() {
    val clientId = "test-client-id"
    val clientSecret = "test-client-secret"
    val dataplaneId = UUID.randomUUID()
    val entity =
      DataplaneClientCredentials(
        id = UUID.randomUUID(),
        dataplaneId = dataplaneId,
        clientId = clientId,
        clientSecret = clientSecret,
        createdBy = UUID.randomUUID(),
      )

    every { dataplaneClientCredentialsRepository.findByClientId(clientId) } returns entity
    every { jwtTokenGenerator.generateToken(any()) } returns Optional.of("test-token")
    every { dataplanePasswordEncoder.matches(any(), any()) } returns true

    val token = service.getToken(clientId, clientSecret)
    token shouldBe "test-token"
  }

  @Test
  fun `should throw when credentials not found for token generation`() {
    val clientId = "wrong-client-id"
    val clientSecret = "wrong-secret"

    every { dataplaneClientCredentialsRepository.findByClientId(clientId) } returns null

    assertThrows<IllegalArgumentException> {
      service.getToken(clientId, clientSecret)
    }
  }
}
