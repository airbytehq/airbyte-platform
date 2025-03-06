/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.hashing

interface Hasher {
  fun hash(
    value: String,
    salt: String? = null,
  ): String
}
