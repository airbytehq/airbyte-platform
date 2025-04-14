/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.DataplaneClientCredentials
import java.util.UUID

/**
 * Provides an abstraction for creating, managing, and retrieving dataplane authentication credentials.
 */
interface DataplaneCredentialsService {
  /**
   * Creates credentials for a given dataplane. A new client ID and secret are generated and
   * persisted, ensuring that the provided [dataplaneId] is associated with valid authentication.
   *
   * @param dataplaneId The unique identifier of the dataplane for which credentials are being created.
   * @return A [DataplaneClientCredentials] object containing the created credentials.
   */
  fun createCredentials(dataplaneId: UUID): DataplaneClientCredentials

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
   * Retrieves the dataplane id associated with a client id
   *
   * @param clientId The client identifier used to obtain the token.
   * @return the dataplaneId
   */
  fun getDataplaneId(clientId: String): UUID
}
