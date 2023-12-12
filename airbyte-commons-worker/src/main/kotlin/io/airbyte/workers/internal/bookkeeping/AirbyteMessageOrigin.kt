/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.internal.bookkeeping

/**
 * Represents the origin of an [io.airbyte.protocol.models.AirbyteMessage].
 */
enum class AirbyteMessageOrigin {
  /**
   * The origin is a destination connector in a sync.
   */
  DESTINATION,

  /**
   * The origin is a source connector in a sync.
   */
  SOURCE,

  /**
   * The origin is internal to the platform in a sync.
   */
  INTERNAL,
}
