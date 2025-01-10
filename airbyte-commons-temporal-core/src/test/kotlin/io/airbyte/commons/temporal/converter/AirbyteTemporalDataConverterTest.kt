/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.converter

import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.common.converter.PayloadConverter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

internal class AirbyteTemporalDataConverterTest {
  @Test
  internal fun testAirbyteTemporalDataConverter() {
    assertEquals(5, AirbyteTemporalDataConverter.payloadConverters.size)
    val jsonPayloadConverter: PayloadConverter? = AirbyteTemporalDataConverter.payloadConverters.find { it is JacksonJsonPayloadConverter }
    assertNotNull(jsonPayloadConverter)
  }
}
