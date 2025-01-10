package io.airbyte.workers.hashing

import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton

@Singleton
@Primary
class Sha256Hasher : Hasher {
  override fun hash(
    value: String,
    salt: String?,
  ): String {
    val bytes = value.toByteArray()
    val md = java.security.MessageDigest.getInstance("SHA-256")
    salt?.let {
      md.update(salt.toByteArray())
    }
    val digest = md.digest(bytes)
    return digest.fold("") { str, it -> str + "%02x".format(it) }
  }
}
