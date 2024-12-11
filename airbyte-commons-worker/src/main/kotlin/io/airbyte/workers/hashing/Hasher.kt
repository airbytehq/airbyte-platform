package io.airbyte.workers.hashing

interface Hasher {
  fun hash(
    value: String,
    salt: String? = null,
  ): String
}
