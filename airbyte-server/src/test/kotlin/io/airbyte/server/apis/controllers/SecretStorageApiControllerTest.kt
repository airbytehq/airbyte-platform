/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.SecretStorageIdRequestBody
import io.airbyte.api.model.generated.SecretStorageRead
import io.airbyte.commons.json.Jsons
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest
internal class SecretStorageApiControllerTest {
  @Factory
  class TestFactory {
    @Singleton
    @Replaces(SecretStorageService::class)
    fun secretStorageService(): SecretStorageService = mockk()
  }

  @Inject
  lateinit var secretStorageService: SecretStorageService

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Test
  fun testGetSecretStorageById() {
    val secretStorageId = SecretStorageId(UUID.randomUUID())
    val secretStorage =
      SecretStorage(
        id = secretStorageId,
        scopeType = SecretStorageScopeType.WORKSPACE,
        scopeId = UUID.randomUUID(),
        configuredFromEnvironment = false,
        storageType = SecretStorageType.GOOGLE_SECRET_MANAGER,
        descriptor = "my-storage",
        createdBy = UUID.randomUUID(),
        updatedBy = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
    every { secretStorageService.getById(secretStorageId) } returns secretStorage

    val storageWithConfig =
      SecretStorageWithConfig(
        secretStorage = secretStorage,
        config = Jsons.jsonNode(mapOf("key" to "value")),
      )
    every { secretStorageService.hydrateStorageConfig(secretStorage) } returns storageWithConfig

    val path = "/api/v1/secret_storage/get"
    val request = HttpRequest.POST(path, SecretStorageIdRequestBody().secretStorageId(secretStorageId.value))
    val response: HttpResponse<SecretStorageRead> = client.toBlocking().exchange(request, SecretStorageRead::class.java)

    assertStatus(HttpStatus.OK, response.status)

    val body = response.body()
    assertNotNull(body)
    assertEquals(secretStorageId.value, body.id)
    assertEquals("value", body.config.get("key").asText())
    assertEquals(secretStorage.scopeId, body.scopeId)
    assertEquals(io.airbyte.api.model.generated.ScopeType.WORKSPACE, body.scopeType)
    assertEquals(io.airbyte.api.model.generated.SecretStorageType.GOOGLE_SECRET_MANAGER, body.secretStorageType)
    assertFalse(body.isConfiguredFromEnvironment)
  }
}
