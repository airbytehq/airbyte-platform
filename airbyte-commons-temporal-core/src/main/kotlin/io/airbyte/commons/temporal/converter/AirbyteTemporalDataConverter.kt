/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.converter

import io.airbyte.commons.jackson.MoreMappers
import io.temporal.common.converter.ByteArrayPayloadConverter
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter
import io.temporal.common.converter.NullPayloadConverter
import io.temporal.common.converter.PayloadConverter
import io.temporal.common.converter.ProtobufJsonPayloadConverter
import io.temporal.common.converter.ProtobufPayloadConverter
import jakarta.inject.Singleton

/**
 * Custom Temporal {@link DataConverter} that modifies the Jackson-based converter to enable
 * case-insensitive enum value parsing by Jackson when loading a job history as part of the test.
 */
@Singleton
class AirbyteTemporalDataConverter : DefaultDataConverter(*payloadConverters) {
  companion object {
    var payloadConverters: Array<PayloadConverter> =
      arrayOf(
        NullPayloadConverter(),
        ByteArrayPayloadConverter(),
        ProtobufJsonPayloadConverter(),
        ProtobufPayloadConverter(),
        JacksonJsonPayloadConverter(MoreMappers.configure(JacksonJsonPayloadConverter.newDefaultObjectMapper())),
      )
  }
}
