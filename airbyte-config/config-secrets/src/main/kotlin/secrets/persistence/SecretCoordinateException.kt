/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package secrets.persistence

class SecretCoordinateException(
  message: String,
  val secretStoreType: String? = null,
  cause: Exception? = null,
) : RuntimeException(message, cause)
