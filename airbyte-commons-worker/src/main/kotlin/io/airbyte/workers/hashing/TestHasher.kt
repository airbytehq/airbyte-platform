/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hashing

import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

@Singleton
@Secondary
class TestHasher : Hasher {
  override fun hash(
    value: String,
    salt: String?,
  ): String = value
}
