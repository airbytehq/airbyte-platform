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
    logger.info { "LOCAL MODE (No-op): Would have deleted user by email on non $realmToKeep realms: $email" }
  }

  override fun getRealmByAuthUserId(authUserId: String): String? = null
}
