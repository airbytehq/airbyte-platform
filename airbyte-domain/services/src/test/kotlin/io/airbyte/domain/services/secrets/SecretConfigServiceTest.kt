/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.data.services.SecretConfigService as SecretConfigRepository

class SecretConfigServiceTest {
  private val secretConfigRepository: SecretConfigRepository = mockk()
  private val service = SecretConfigService(secretConfigRepository)

  @Nested
  inner class GetById {
    @Test
    fun `should return secret config when found`() {
      val id = SecretConfigId(UUID.randomUUID())
      val secretConfig = mockk<SecretConfig>()
      every { secretConfigRepository.findById(id) } returns secretConfig

      service.getById(id) shouldBe secretConfig
    }

    @Test
    fun `should throw ResourceNotFoundProblem when ID does not exist`() {
      val id = SecretConfigId(UUID.randomUUID())
      every { secretConfigRepository.findById(id) } returns null

      shouldThrow<ResourceNotFoundProblem> { service.getById(id) }
    }
  }
}
