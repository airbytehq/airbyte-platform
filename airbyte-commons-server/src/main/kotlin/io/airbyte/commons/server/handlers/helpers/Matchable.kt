/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

internal fun interface Matchable<K> {
  fun match(k: K): K
}
