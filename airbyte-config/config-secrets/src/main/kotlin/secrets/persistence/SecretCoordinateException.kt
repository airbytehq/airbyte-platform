/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package secrets.persistence

class SecretCoordinateException : RuntimeException {
  constructor(message: String, ex: Exception?) : super(message, ex)
  constructor(message: String) : super(message)
  constructor(ex: Exception) : super(ex)
}
