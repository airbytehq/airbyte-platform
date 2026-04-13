/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.keycloak

/**
 * Thrown when Keycloak rejects a client_id / client_secret with HTTP 401.
 * Scoped to credential-rejection only — other Keycloak failure modes (service
 * unreachable, bad realm, etc.) are deliberately not translated and surface as 500s.
 */
class InvalidClientCredentialsException(
  message: String,
  cause: Throwable? = null,
) : RuntimeException(message, cause)
