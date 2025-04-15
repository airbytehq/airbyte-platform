/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

/**
 * Provides an abstraction for generating tokens for dataplane authentication.
 */
interface DataplaneTokenService {
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
