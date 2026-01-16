/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test

import io.micronaut.context.annotation.ConfigurationProperties
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

@ConfigurationProperties("airbyte.test")
data class TestClass(
  val booleanValue: Boolean = true,
  val intValue: Int = 1,
  val longValue: Long = 2L,
  val floatValue: Float = .3F,
  val doubleValue: Double = 4.4,
  val durationValue: Duration = Duration.parse("PT15M"),
  val pathValue: Path = Path.of("/some/path"),
  val stringValue: String = "Hello World!",
  val uuidValue: UUID = UUID.randomUUID(),
)
