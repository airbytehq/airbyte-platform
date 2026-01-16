/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

@MicronautTest
internal class DeclarativeManifestImageVersionRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    val declarativeManifestImageVersion0 =
      DeclarativeManifestImageVersion(
        majorVersion = 0,
        imageVersion = "0.79.0",
        imageSha = "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33",
      )
  }

  @AfterEach
  fun tearDown() {
    declarativeManifestImageVersionRepository.deleteAll()
  }

  @Test
  fun `test create declarative manifest image version and get by version`() {
    declarativeManifestImageVersionRepository.save(declarativeManifestImageVersion0)
    assert(declarativeManifestImageVersionRepository.count() == 1L)

    val persistedCdkVersion = declarativeManifestImageVersionRepository.findById(0).get()
    assertVersionsAreEqual(declarativeManifestImageVersion0, persistedCdkVersion)
  }

  @Test
  fun `test get for nonexistent version`() {
    assert(declarativeManifestImageVersionRepository.findById(1).isEmpty)
  }

  @Test
  fun `test update active version`() {
    declarativeManifestImageVersionRepository.save(declarativeManifestImageVersion0)
    val initialPersistedCdkVersion = declarativeManifestImageVersionRepository.findById(0).get()

    val newActiveVersion =
      DeclarativeManifestImageVersion(
        majorVersion = 0,
        imageVersion = "0.80.0",
        "sha256:a54aad18cf460173f753fe938e254a667dac97b703fc05cf6de8c839caf62ef4",
      )
    declarativeManifestImageVersionRepository.update(newActiveVersion)
    val updatedPersistedCdkVersion = declarativeManifestImageVersionRepository.findById(0).get()

    assertVersionWasUpdated(initialPersistedCdkVersion, updatedPersistedCdkVersion)
  }

  @Test
  fun `test insert multiple active versions`() {
    val declarativeManifestImageVersion1 =
      DeclarativeManifestImageVersion(
        majorVersion = 1,
        imageVersion = "1.0.4",
        "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c",
      )
    declarativeManifestImageVersionRepository.save(declarativeManifestImageVersion0)
    declarativeManifestImageVersionRepository.save(declarativeManifestImageVersion1)

    assert(declarativeManifestImageVersionRepository.count() == 2L)

    val persistedCdkVersion = declarativeManifestImageVersionRepository.findById(0).get()
    assertVersionsAreEqual(declarativeManifestImageVersion0, persistedCdkVersion)

    val persistedCdkVersion2 = declarativeManifestImageVersionRepository.findById(1).get()
    assertVersionsAreEqual(declarativeManifestImageVersion1, persistedCdkVersion2)
  }

  @Test
  fun `test get multiple active versions`() {
    val declarativeManifestImageVersion1 =
      DeclarativeManifestImageVersion(
        majorVersion = 1,
        imageVersion = "1.0.4",
        "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c",
      )
    declarativeManifestImageVersionRepository.save(declarativeManifestImageVersion0)
    declarativeManifestImageVersionRepository.save(declarativeManifestImageVersion1)

    val persistedCdkVersions = declarativeManifestImageVersionRepository.findAll()
    assert(persistedCdkVersions.size == 2)
    assertVersionsAreEqual(declarativeManifestImageVersion0, persistedCdkVersions[0])
    assertVersionsAreEqual(declarativeManifestImageVersion1, persistedCdkVersions[1])
  }

  private fun assertVersionsAreEqual(
    expected: DeclarativeManifestImageVersion,
    actual: DeclarativeManifestImageVersion,
  ) {
    assert(expected.majorVersion == actual.majorVersion)
    assert(expected.imageVersion == actual.imageVersion)
  }

  /*
   * Asserts that the version was updated correctly. Use comparison on persisted versions only
   * to be able to compare the timestamps.
   */
  private fun assertVersionWasUpdated(
    oldVersion: DeclarativeManifestImageVersion,
    newVersion: DeclarativeManifestImageVersion,
  ) {
    assert(oldVersion.majorVersion == newVersion.majorVersion)
    assert(oldVersion.imageVersion != newVersion.imageVersion)
    assert(oldVersion.createdAt == newVersion.createdAt)
    assert(oldVersion.updatedAt!! < newVersion.updatedAt!!)
  }
}
