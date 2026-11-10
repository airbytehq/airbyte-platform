/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AirbyteRelease
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset

@MicronautTest
class AirbyteReleaseRepositoryTest : AbstractConfigRepositoryTest() {
  private val repository = context.getBean(AirbyteReleaseRepository::class.java)

  @AfterEach
  fun cleanUp() {
    repository.deleteAll()
  }

  @Test
  fun `findByVersionIn returns only the recorded versions among those requested`() {
    repository.save(AirbyteRelease(version = "2.0.0", sourceCommit = COMMIT_A, publishedAt = PUBLISHED_AT))
    repository.save(AirbyteRelease(version = "2.1.0", sourceCommit = COMMIT_B, publishedAt = PUBLISHED_AT.plusDays(60)))
    repository.save(AirbyteRelease(version = "2.2.0", sourceCommit = COMMIT_A, publishedAt = PUBLISHED_AT.plusDays(120)))

    val found = repository.findByVersionIn(listOf("2.0.0", "2.2.0", "9.9.9", "dev-0123456789"))

    assertEquals(
      listOf("2.0.0" to PUBLISHED_AT.toInstant(), "2.2.0" to PUBLISHED_AT.plusDays(120).toInstant()),
      found.map { it.version to it.publishedAt.toInstant() }.sortedBy { it.first },
    )
  }

  @Test
  fun `findByVersionIn returns an empty list for an empty table or empty input`() {
    assertTrue(repository.findByVersionIn(listOf("2.1.0")).isEmpty())

    repository.save(AirbyteRelease(version = "2.1.0", sourceCommit = COMMIT_A, publishedAt = PUBLISHED_AT))

    val found = repository.findByVersionIn(emptyList())
    assertTrue(found.isEmpty(), "$found")
  }

  companion object {
    private const val COMMIT_A = "0123456789abcdef0123456789abcdef01234567"
    private const val COMMIT_B = "89abcdef0123456789abcdef0123456789abcdef"
    private val PUBLISHED_AT = OffsetDateTime.of(2026, 11, 6, 17, 30, 0, 0, ZoneOffset.UTC)
  }
}
