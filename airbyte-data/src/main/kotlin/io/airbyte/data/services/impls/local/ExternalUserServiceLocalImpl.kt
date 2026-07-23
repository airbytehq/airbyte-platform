/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.local

import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.data.services.ExternalUserService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

private val logger = KotlinLogging.logger { }

@Singleton
@Primary
@Requires(env = [EnvConstants.LOCAL_TEST])
class ExternalUserServiceLocalImpl : ExternalUserService {
  override fun deleteUserByExternalId(
    authUserId: String,
    realm: String,
  ) {
    logger.info { "LOCAL MODE (No-op): Would have deleted user by external id: $authUserId on $realm" }
  }

  override fun deleteUserByEmailOnOtherRealms(
    email: String,
    realmToKeep: String,
  ) {
    logger.info { "LOCAL MODE (No-op): Would have deleted user by emailHash=${emailHash(email)} on non $realmToKeep realms" }
  }

  override fun findUsersByEmailInRealm(
    email: String,
    realm: String,
  ): List<ExternalUserService.ExternalUser> {
    logger.info { "LOCAL MODE (No-op): Would have looked up users by emailHash=${emailHash(email)} in $realm" }
    return emptyList()
  }

  override fun deleteUsersByEmailInRealm(
    email: String,
    realm: String,
  ): Int {
    logger.info { "LOCAL MODE (No-op): Would have deleted users by emailHash=${emailHash(email)} in $realm" }
    return 0
  }

  override fun getRealmByAuthUserId(authUserId: String): String? = null

  private fun emailHash(email: String): String {
    val digest =
      MessageDigest
        .getInstance("SHA-256")
        .digest(email.trim().lowercase(Locale.US).toByteArray(StandardCharsets.UTF_8))
    return digest.joinToString("") { "%02x".format(it.toInt() and 0xff) }
  }
}
