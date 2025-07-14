/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import io.airbyte.commons.version.Version

/**
 * Exception for when a connector is using a protocol version that the current version of the
 * platform does not support.
 */
class UnsupportedProtocolVersionException(
  current: String?,
  minSupported: Version,
  maxSupported: Version,
) : KnownException(
    String.format(
      "Airbyte Protocol Version %s is not supported. (Must be within [%s:%s])",
      current,
      minSupported.serialize(),
      maxSupported.serialize(),
    ),
  ) {
  constructor(current: Version, minSupported: Version, maxSupported: Version) : this(current.serialize(), minSupported, maxSupported)

  override fun getHttpCode(): Int = 400
}
