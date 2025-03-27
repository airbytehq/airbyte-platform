/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.DataplaneClientCredentials
import io.airbyte.data.services.DataplaneAuthService
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

/**
 * A non-secure implementation of DataplaneAuthService for environments where security is disabled.
 */
@Singleton
@Requires(property = "micronaut.security.enabled", notEquals = "true")
class DataplaneAuthServiceNoAuthImpl : DataplaneAuthService {
  override fun createCredentials(dataplaneId: UUID): DataplaneClientCredentials =
    DataplaneClientCredentials(
      id = UUID.randomUUID(),
      dataplaneId = dataplaneId,
      clientId = "non-secure-client-${UUID.randomUUID()}",
      clientSecret = "non-secure-secret-${UUID.randomUUID()}",
      createdAt = OffsetDateTime.now(),
    )

  override fun deleteCredentials(dataplaneClientCredentialsId: UUID): DataplaneClientCredentials? = null

  override fun listCredentialsByDataplaneId(dataplaneId: UUID): List<DataplaneClientCredentials> = emptyList()

  override fun getToken(
    clientId: String,
    clientSecret: String,
  ): String = "non-secure-token-${UUID.randomUUID()}"

  override fun getDataplaneId(clientId: String): UUID =
    TODO(
      """
      In a no-auth environment we need to decide if:
        1) We want to initialize the same way and therefore want to create credentials per dataplane.
        2) Initialize in a separate fashion and therefore never do this look-up.
      """.trimIndent(),
    )
}
