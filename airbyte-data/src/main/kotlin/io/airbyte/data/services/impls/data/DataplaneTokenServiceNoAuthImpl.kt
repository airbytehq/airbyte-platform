/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.services.DataplaneTokenService
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

/**
 * A non-secure implementation of DataplaneTokenService for environments where security is disabled.
 */
@Singleton
@Requires(property = "micronaut.security.enabled", notEquals = "true")
class DataplaneTokenServiceNoAuthImpl : DataplaneTokenService {
  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String = "non-secure-token-${UUID.randomUUID()}"
}
