/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.DataplaneClientCredentials
import java.util.UUID

/**
 * Provides an abstraction for managing and retrieving dataplane authentication credentials,
 * including creation, deletion, and token generation.
 */
interface DataplaneAuthService {
  /**
   * Creates credentials for a given dataplane. A new client ID and secret are generated and
   * persisted, ensuring that the provided [dataplaneId] is associated with valid authentication.
   *
   * @param dataplaneId The unique identifier of the dataplane for which credentials are being created.
   * @param createdById The ID of the user creating these credentials.
   * @return A [DataplaneClientCredentials] object containing the created credentials.
   */
  fun createCredentials(
    dataplaneId: UUID,
    createdById: UUID,
  ): DataplaneClientCredentials

  /**
   * Deletes the credentials associated with the provided [dataplaneClientCredentialsId], if they exist.
   *
   * @param dataplaneClientCredentialsId The unique identifier of the dataplane credentials to be deleted.
   * @return The [DataplaneClientCredentials] that were deleted, or null if no credentials were found.
   */
  fun deleteCredentials(dataplaneClientCredentialsId: UUID): DataplaneClientCredentials?

  /**
   * Retrieves all credentials associated with a specific dataplane.
   *
   * @param dataplaneId The unique identifier of the dataplane.
   * @return A list of [DataplaneClientCredentials], which may be empty if none exist for the given dataplane.
   */
  fun listCredentialsByDataplaneId(dataplaneId: UUID): List<DataplaneClientCredentials>

  /**
   * Obtains an authentication token based on a provided [clientId] and [clientSecret]. The returned
   * token can be used to authenticate subsequent requests.
   *
   * @param clientId The client identifier used to obtain the token.
   * @param clientSecret The secret associated with the [clientId].
   * @return A valid authentication token as a [String].
   */
  fun getToken(
    clientId: String,
    clientSecret: String,
  ): String
}
