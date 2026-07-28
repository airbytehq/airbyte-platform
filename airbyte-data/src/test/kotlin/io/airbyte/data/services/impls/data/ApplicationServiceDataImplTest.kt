/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.repositories.ApplicationRepository
import io.airbyte.data.repositories.ScimAuthUserRepository
import io.airbyte.data.repositories.entities.Application
import io.airbyte.data.services.ScimAuthUserOwnershipService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

internal class ApplicationServiceDataImplTest {
  private val applicationRepository = mockk<ApplicationRepository>()
  private val jwtTokenGenerator = mockk<JwtTokenGenerator>()
  private val airbyteAuthConfig = AirbyteAuthConfig(tokenIssuer = "test-issuer")

  @Test
  fun `application creation loses a DSR reassignment race before persisting credentials`() {
    val user =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId("reassigned-create-subject")
    val ownershipService =
      object : ScimAuthUserOwnershipService(mockk<ScimAuthUserRepository>()) {
        override fun <T> withUniqueOwner(
          authUserId: String,
          expectedUserId: UUID?,
          operation: () -> T,
        ): T = throw IllegalStateException("Authentication identity was reassigned by DSR.")
      }
    every { applicationRepository.save(any()) } answers {
      firstArg<Application>().also { it.createdAt = OffsetDateTime.now() }
    }
    val service =
      ApplicationServiceDataImpl(
        applicationRepository,
        airbyteAuthConfig,
        jwtTokenGenerator,
        ownershipService,
      )

    assertThatThrownBy {
      service.createApplication(user, "must-not-be-created")
    }.isInstanceOf(IllegalStateException::class.java)

    verify(exactly = 0) { applicationRepository.save(any()) }
  }

  @Test
  fun `stale token request revalidates deleted credential after DSR reassigns the subject`() {
    val application =
      Application(
        id = UUID.randomUUID(),
        authUserId = "reassigned-token-subject",
        name = "stale credential",
        clientId = "stale-client-id",
        clientSecret = "stale-client-secret",
        createdAt = OffsetDateTime.now(),
      )
    var ownershipLockAcquired = false
    every {
      applicationRepository.findByClientIdAndClientSecret(application.clientId, application.clientSecret)
    } answers {
      if (ownershipLockAcquired) null else application
    }
    every { jwtTokenGenerator.generateToken(any()) } returns Optional.of("must-not-be-issued")
    val ownershipService =
      object : ScimAuthUserOwnershipService(mockk<ScimAuthUserRepository>()) {
        override fun <T> withUniqueOwner(
          authUserId: String,
          expectedUserId: UUID?,
          operation: () -> T,
        ): T {
          ownershipLockAcquired = true
          return operation()
        }
      }
    val service =
      ApplicationServiceDataImpl(
        applicationRepository,
        airbyteAuthConfig,
        jwtTokenGenerator,
        ownershipService,
      )

    assertThatThrownBy {
      service.getToken(application.clientId, application.clientSecret)
    }.isInstanceOf(IllegalArgumentException::class.java)

    verify(exactly = 2) {
      applicationRepository.findByClientIdAndClientSecret(application.clientId, application.clientSecret)
    }
    verify(exactly = 0) { jwtTokenGenerator.generateToken(any()) }
  }
}
