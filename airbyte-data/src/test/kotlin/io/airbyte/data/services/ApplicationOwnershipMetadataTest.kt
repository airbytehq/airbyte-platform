/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.commons.auth.keycloak.ClientScopeConfigurator
import io.airbyte.data.repositories.ApplicationRepository
import io.airbyte.data.services.impls.data.ApplicationServiceDataImpl
import io.airbyte.data.services.impls.data.ApplicationServiceMicronautImpl
import io.airbyte.data.services.impls.keycloak.ApplicationServiceKeycloakImpl
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteKeycloakConfig
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.BadRequestException
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.keycloak.admin.client.Keycloak
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.admin.client.resource.UsersResource
import org.keycloak.representations.idm.UserRepresentation
import java.io.IOException
import java.util.UUID

class ApplicationOwnershipMetadataTest {
  private val application = UUID(0, 16)

  @Test
  fun `native ownership uses only owner and row existence without loading credentials`() {
    val repository = mockk<ApplicationRepository>()
    val tokens = mockk<JwtTokenGenerator>()
    val ownership = mockk<ScimAuthUserOwnershipService>()
    val provider = ApplicationServiceDataImpl(repository, AirbyteAuthConfig(), tokens, ownership)
    every { repository.existsByAuthUserIdAndId("owner", application) } returns true
    assertDoesNotThrow { provider.requireApplicationOwner("owner", application) }
    every { repository.existsByAuthUserIdAndId("owner", application) } returns false
    assertThrows(IllegalArgumentException::class.java) { provider.requireApplicationOwner("owner", application) }
    val failure = IOException("application ownership unavailable")
    every { repository.existsByAuthUserIdAndId("owner", application) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { provider.requireApplicationOwner("owner", application) })
    verify(exactly = 0) { repository.findByAuthUserIdAndId(any(), any()) }
    verify(exactly = 0) { repository.findByAuthUserId(any()) }
    verify(exactly = 0) { repository.findByClientIdAndClientSecret(any(), any()) }
    verify(exactly = 0) { tokens.generateToken(any()) }
    verify(exactly = 0) { ownership.uniqueOwner(any()) }
  }

  @Test
  fun `OIDC ownership uses bounded exact service-user metadata and never client representations`() {
    val keycloak = mockk<Keycloak>()
    val realm = mockk<RealmResource>()
    val users = mockk<UsersResource>()
    every { keycloak.realm("fixture") } returns realm
    every { realm.users() } returns users
    val provider =
      ApplicationServiceKeycloakImpl(
        keycloak,
        AirbyteKeycloakConfig(protocol = "http", host = "localhost:8080", clientRealm = "fixture"),
        mockk<ClientScopeConfigurator>(),
        AirbyteAuthConfig(),
        mockk(),
      )
    val owner = "opaque|owner with:query grammar"
    val record = UserRepresentation().apply { attributes = mapOf("client_id" to listOf(application.toString()), "user_id" to listOf(owner)) }
    every { users.searchByAttributes(0, 2, null, false, "client_id:$application") } returns listOf(record)
    assertDoesNotThrow { provider.requireApplicationOwner(owner, application) }
    assertThrows(BadRequestException::class.java) { provider.requireApplicationOwner("another-owner", application) }
    every { users.searchByAttributes(0, 2, null, false, "client_id:$application") } returns emptyList()
    assertThrows(NoSuchElementException::class.java) { provider.requireApplicationOwner(owner, application) }
    every { users.searchByAttributes(0, 2, null, false, "client_id:$application") } returns listOf(record, record)
    assertThrows(NoSuchElementException::class.java) { provider.requireApplicationOwner(owner, application) }
    for (attributes in listOf(
      mapOf("client_id" to listOf(UUID(0, 99).toString()), "user_id" to listOf(owner)),
      mapOf("client_id" to listOf(application.toString(), application.toString()), "user_id" to listOf(owner)),
      mapOf("client_id" to listOf(application.toString()), "user_id" to listOf(owner, owner)),
      mapOf("client_id" to listOf(application.toString())),
    )) {
      val malformed = UserRepresentation().apply { this.attributes = attributes }
      every { users.searchByAttributes(0, 2, null, false, "client_id:$application") } returns listOf(malformed)
      assertThrows(NoSuchElementException::class.java) { provider.requireApplicationOwner(owner, application) }
    }
    val failure = IOException("OIDC unavailable")
    every { users.searchByAttributes(0, 2, null, false, "client_id:$application") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { provider.requireApplicationOwner(owner, application) })
    verify(exactly = 0) { realm.clients() }
  }

  @Test
  fun `SIMPLE application ownership remains explicitly unsupported`() {
    val tokens = mockk<JwtTokenGenerator>()
    val provider = ApplicationServiceMicronautImpl(AirbyteAuthConfig(), tokens)
    assertThrows(UnsupportedOperationException::class.java) { provider.requireApplicationOwner("owner", application) }
    verify(exactly = 0) { tokens.generateToken(any()) }
  }
}
