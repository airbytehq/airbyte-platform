/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Application
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class ApplicationOwnershipRepositoryTest : AbstractConfigRepositoryTest() {
  @Test
  fun `application existence requires both exact owner and exact row id`() {
    val repository = context.getBean(ApplicationRepository::class.java)
    val first = UUID.randomUUID()
    val second = UUID.randomUUID()
    repository.save(
      Application(id = first, authUserId = "first-owner", name = "first", clientId = UUID.randomUUID().toString(), clientSecret = "fixture"),
    )
    repository.save(
      Application(id = second, authUserId = "second-owner", name = "second", clientId = UUID.randomUUID().toString(), clientSecret = "fixture"),
    )
    assertTrue(repository.existsByAuthUserIdAndId("first-owner", first))
    assertTrue(repository.existsByAuthUserIdAndId("second-owner", second))
    assertFalse(repository.existsByAuthUserIdAndId("first-owner", second))
    assertFalse(repository.existsByAuthUserIdAndId("second-owner", first))
    assertFalse(repository.existsByAuthUserIdAndId("missing-owner", first))
    assertFalse(repository.existsByAuthUserIdAndId("first-owner", UUID(0, 0)))
  }
}
