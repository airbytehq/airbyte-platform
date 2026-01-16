/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.DeclarativeManifestImageVersionRepository
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.mockk.clearAllMocks
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional

internal class DeclarativeManifestImageVersionServiceDataImplTest {
  private val declarativeManifestImageVersionRepository = mockk<DeclarativeManifestImageVersionRepository>()
  private val declarativeManifestImageVersionService = DeclarativeManifestImageVersionServiceDataImpl(declarativeManifestImageVersionRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test write new declarative manifest image version`() {
    val majorVersion = 0
    val version = DeclarativeManifestImageVersion(majorVersion, "0.0.1", "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33")

    every { declarativeManifestImageVersionRepository.existsById(majorVersion) } returns false
    every { declarativeManifestImageVersionRepository.save(version) } returns version

    val result = declarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(version)
    assert(result == version)

    verify {
      declarativeManifestImageVersionRepository.existsById(majorVersion)
      declarativeManifestImageVersionRepository.save(version)
    }
    confirmVerified(declarativeManifestImageVersionRepository)
  }

  @Test
  fun `test write existing declarative manifest image version`() {
    val majorVersion = 0
    val newVersion = DeclarativeManifestImageVersion(majorVersion, "0.0.2", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c")

    every { declarativeManifestImageVersionRepository.existsById(majorVersion) } returns true
    every { declarativeManifestImageVersionRepository.update(newVersion) } returns newVersion

    val result = declarativeManifestImageVersionService.writeDeclarativeManifestImageVersion(newVersion)
    assert(result == newVersion)

    verify {
      declarativeManifestImageVersionRepository.existsById(majorVersion)
      declarativeManifestImageVersionRepository.update(newVersion)
    }
    confirmVerified(declarativeManifestImageVersionRepository)
  }

  @Test
  fun `test get declarative manifest image version by major version`() {
    val majorVersion = 0
    val version = DeclarativeManifestImageVersion(majorVersion, "0.0.1", "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33")

    every { declarativeManifestImageVersionRepository.findById(majorVersion) } returns Optional.of(version)

    val result = declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(majorVersion)
    assert(result == version)

    verify {
      declarativeManifestImageVersionRepository.findById(majorVersion)
    }
    confirmVerified(declarativeManifestImageVersionRepository)
  }

  @Test
  fun `test get declarative manifest image version by major version not found`() {
    val majorVersion = 0

    every { declarativeManifestImageVersionRepository.findById(majorVersion) } returns Optional.empty()

    assertThrows<IllegalStateException> { declarativeManifestImageVersionService.getDeclarativeManifestImageVersionByMajorVersion(majorVersion) }

    verify {
      declarativeManifestImageVersionRepository.findById(majorVersion)
    }
    confirmVerified(declarativeManifestImageVersionRepository)
  }

  @Test
  fun `test list declarative manifest image versions`() {
    val declarativeManifestImageVersion0 =
      DeclarativeManifestImageVersion(0, "0.0.1", "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33")
    val declarativeManifestImageVersion1 =
      DeclarativeManifestImageVersion(1, "1.0.0", "sha256:a54aad18cf460173f753fe938e254a667dac97b703fc05cf6de8c839caf62ef4")
    val declarativeManifestImageVersion2 =
      DeclarativeManifestImageVersion(2, "2.0.0", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c")

    every {
      declarativeManifestImageVersionRepository.findAll()
    } returns listOf(declarativeManifestImageVersion0, declarativeManifestImageVersion1, declarativeManifestImageVersion2)
    val result = declarativeManifestImageVersionService.listDeclarativeManifestImageVersions()
    assert(result == listOf(declarativeManifestImageVersion0, declarativeManifestImageVersion1, declarativeManifestImageVersion2))
  }
}
