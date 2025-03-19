/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.sift.AbstractDiscriminator

/**
 * Custom Logback [ch.qos.logback.core.sift.Discriminator] implementation that uses the
 * job log path MDC value as a discriminator for appender creation.
 */
class AirbyteStorageMDCBasedDiscriminator(
  private val mdcValueExtractor: (Map<String, String>) -> String,
) : AbstractDiscriminator<ILoggingEvent>() {
  // Not implemented/not used.
  override fun getKey(): String = ""

  override fun getDiscriminatingValue(event: ILoggingEvent): String = mdcValueExtractor(event.mdcPropertyMap)
}
