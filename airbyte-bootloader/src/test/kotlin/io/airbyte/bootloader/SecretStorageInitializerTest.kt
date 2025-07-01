/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.config.secrets.persistence.SecretPersistence.ImplementationTypes
import io.airbyte.data.services.SecretStorageService
import io.airbyte.domain.models.PatchField
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.UserId
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifySequence
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class SecretStorageInitializerTest {
  private val secretStorageService = mockk<SecretStorageService>(relaxed = true)
  private val defaultId = SecretStorage.DEFAULT_SECRET_STORAGE_ID
  private val defaultOrgId = DEFAULT_ORGANIZATION_ID
  private val defaultUserId = DEFAULT_USER_ID
  private val defaultDescriptor = "Default Secret Storage"

  @BeforeEach
  fun setup() {
    clearAllMocks()
  }

  private fun makeDomain(storageType: SecretStorageType) =
    SecretStorage(
      id = defaultId,
      scopeType = SecretStorageScopeType.ORGANIZATION,
      scopeId = defaultOrgId,
      descriptor = defaultDescriptor,
      storageType = storageType,
      configuredFromEnvironment = true,
      tombstone = false,
      createdBy = UUID.randomUUID(),
      updatedBy = UUID.randomUUID(),
      createdAt = null,
      updatedAt = null,
    )

  @Test
  fun `when none exists, initializer should CREATE with mapped type`() {
    every { secretStorageService.findById(defaultId) } returns null

    val createSlot = slot<SecretStorageCreate>()
    every { secretStorageService.create(capture(createSlot)) } returns makeDomain(SecretStorageType.AWS_SECRETS_MANAGER)

    val init = SecretStorageInitializer(secretStorageService, ImplementationTypes.AWS_SECRET_MANAGER)
    init.createOrUpdateDefaultSecretStorage()

    verifySequence {
      secretStorageService.findById(defaultId)
      secretStorageService.create(any())
    }

    val captured = createSlot.captured
    captured.scopeType shouldBe SecretStorageScopeType.ORGANIZATION
    captured.scopeId shouldBe defaultOrgId
    captured.descriptor shouldBe defaultDescriptor
    captured.storageType shouldBe SecretStorageType.AWS_SECRETS_MANAGER
    captured.configuredFromEnvironment shouldBe true
    captured.createdBy shouldBe UserId(defaultUserId)
  }

  @Test
  fun `when one exists with matching type, initializer should do nothing`() {
    every { secretStorageService.findById(defaultId) } returns makeDomain(SecretStorageType.GOOGLE_SECRET_MANAGER)

    val init = SecretStorageInitializer(secretStorageService, ImplementationTypes.GOOGLE_SECRET_MANAGER)
    init.createOrUpdateDefaultSecretStorage()

    verify(exactly = 1) { secretStorageService.findById(defaultId) }
    verify(exactly = 0) { secretStorageService.create(any()) }
    verify(exactly = 0) {
      secretStorageService.patch(
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }
  }

  @Test
  fun `when one exists with different type, initializer should PATCH with toPatch`() {
    every { secretStorageService.findById(defaultId) } returns makeDomain(SecretStorageType.LOCAL_TESTING)

    val init = SecretStorageInitializer(secretStorageService, ImplementationTypes.VAULT)
    init.createOrUpdateDefaultSecretStorage()

    verifySequence {
      secretStorageService.findById(defaultId)
      secretStorageService.patch(
        id = defaultId,
        updatedBy = UserId(defaultUserId),
        storageType = PatchField.Present(SecretStorageType.VAULT),
      )
    }
  }
}
