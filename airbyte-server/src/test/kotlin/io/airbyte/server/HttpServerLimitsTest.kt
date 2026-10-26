/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server

import io.micronaut.http.server.HttpServerConfiguration
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Internal clients post discovered catalogs, per-stream state and job outputs to this server.
 * Both Micronaut body limits must allow 50MB, and they must agree: since Micronaut 4.5 the
 * buffered-body limit is a separate property, and a body over either one is rejected with 413.
 */
@MicronautTest
internal class HttpServerLimitsTest {
  @Inject
  lateinit var serverConfiguration: HttpServerConfiguration

  @Test
  fun `request body limits are 50MB and agree with each other`() {
    val fiftyMegabytes = 52_428_800L
    assertEquals(fiftyMegabytes, serverConfiguration.maxRequestSize)
    assertEquals(fiftyMegabytes, serverConfiguration.maxRequestBufferSize)
  }
}
