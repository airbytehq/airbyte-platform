/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ScopeType
import io.airbyte.api.model.generated.SecretStorageCreateRequestBody
import io.airbyte.api.model.generated.SecretStorageIdRequestBody
import io.airbyte.api.model.generated.SecretStorageListRequestBody
import io.airbyte.api.model.generated.SecretStorageRead
import io.airbyte.api.model.generated.SecretStorageReadList
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.UserId
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.server.assertStatus
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@MicronautTest(rebuildContext = true)
internal class SecretStorageApiControllerTest {
  @Factory
  class TestFactory {
    @Singleton
    @Replaces(SecretStorageService::class)
    fun secretStorageService(): SecretStorageService = mockk()

    @Singleton
    @Replaces(RoleResolver::class)
    fun roleResolver(): RoleResolver = mockk(relaxed = true)
  }

  @Inject
  private lateinit var currentUserService: CurrentUserService

  @Inject
  lateinit var secretStorageService: SecretStorageService

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(CurrentUserService::class)
  fun currentUserService(): CurrentUserService = mockk()

  private val roleResolver = mockk<RoleResolver>()

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

  @Test
  fun testListSecretStorage() {
    val secretStorageId = SecretStorageId(UUID.randomUUID())
    val scopeId = UUID.randomUUID()
    val secretStorage =
      SecretStorage(
        id = secretStorageId,
        scopeType = SecretStorageScopeType.WORKSPACE,
        scopeId = scopeId,
        configuredFromEnvironment = false,
        storageType = SecretStorageType.GOOGLE_SECRET_MANAGER,
        descriptor = "my-storage",
        createdBy = UUID.randomUUID(),
        updatedBy = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
    every { secretStorageService.listSecretStorage(SecretStorageScopeType.WORKSPACE, scopeId) } returns listOf(secretStorage)

    val path = "/api/v1/secret_storage/list"
    val request = HttpRequest.POST(path, SecretStorageListRequestBody().scopeType(ScopeType.WORKSPACE).scopeId(scopeId))
    val response: HttpResponse<SecretStorageReadList> = client.toBlocking().exchange(request, SecretStorageReadList::class.java)

    assertStatus(HttpStatus.OK, response.status)

    val body = response.body()
    assertNotNull(body)
    assertEquals(1, body.secretStorages.size)
    assertEquals(secretStorageId.value, body.secretStorages[0].id)
  }

  @Test
  fun testCreateSecretStorage() {
    val currentUserId = UUID.randomUUID()
    val secretStorage =
      SecretStorage(
        id = SecretStorageId(UUID.randomUUID()),
        scopeType = SecretStorageScopeType.ORGANIZATION,
        scopeId = UUID.randomUUID(),
        configuredFromEnvironment = false,
        storageType = SecretStorageType.AWS_SECRETS_MANAGER,
        descriptor = "My Storage",
        createdBy = currentUserId,
        updatedBy = currentUserId,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
    val storageConfig = Jsons.jsonNode(mapOf("key" to "value"))

    every {
      secretStorageService.createSecretStorage(
        SecretStorageCreate(
          scopeType = SecretStorageScopeType.ORGANIZATION,
          scopeId = secretStorage.scopeId,
          storageType = SecretStorageType.AWS_SECRETS_MANAGER,
          descriptor = "My Storage",
          configuredFromEnvironment = false,
          createdBy = UserId(currentUserId),
        ),
        storageConfig,
      )
    } returns secretStorage

    every {
      currentUserService.getCurrentUser()
    } returns
      mockk {
        every { userId } returns currentUserId
      }

    val path = "/api/v1/secret_storage/create"
    val requestBody =
      SecretStorageCreateRequestBody()
        .descriptor("My Storage")
        .config(storageConfig)
        .scopeType(ScopeType.ORGANIZATION)
        .scopeId(secretStorage.scopeId)
        .secretStorageType(io.airbyte.api.model.generated.SecretStorageType.AWS_SECRETS_MANAGER)
    val request = HttpRequest.POST(path, requestBody)
    val response: HttpResponse<SecretStorageRead> = client.toBlocking().exchange(request, SecretStorageRead::class.java)
    assertStatus(HttpStatus.OK, response.status)

    val body = response.body()
    assertNotNull(body)
    assertEquals(requestBody.scopeId, body.scopeId)
    assertEquals(ScopeType.ORGANIZATION, body.scopeType)
    assertEquals(io.airbyte.api.model.generated.SecretStorageType.AWS_SECRETS_MANAGER, body.secretStorageType)
    assertNull(body.config)
  }
}
