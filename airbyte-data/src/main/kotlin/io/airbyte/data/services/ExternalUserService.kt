/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

/**
 * A service that manages users in external services, such as Keycloak or Firebase.
 * These external users are related to Airbyte's User concept through the authUserId and authProvider.
 */
interface ExternalUserService {
  fun deleteUserByExternalId(
    authUserId: String,
    realm: String,
  )

  fun deleteUserByEmailOnOtherRealms(
    email: String,
    realmToKeep: String,
  )

  fun getRealmByAuthUserId(authUserId: String): String?
}
