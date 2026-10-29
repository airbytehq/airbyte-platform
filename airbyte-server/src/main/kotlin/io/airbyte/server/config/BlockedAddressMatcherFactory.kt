/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.commons.net.BlockedAddressMatcher
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class BlockedAddressMatcherFactory {
  /** The server only checks IP literals against the default ranges; the data plane agent enforces the full policy. */
  @Singleton
  fun blockedAddressMatcher(): BlockedAddressMatcher = BlockedAddressMatcher()
}
