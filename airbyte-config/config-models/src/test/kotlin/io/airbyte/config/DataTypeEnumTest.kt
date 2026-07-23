/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.protocol.models.JsonSchemaPrimitiveUtil.JsonSchemaPrimitive
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Locale

internal class DataTypeEnumTest {
  // We use JsonSchemaPrimitive in tests to construct schemas. We want to verify that there are valid
  // conversions between JsonSchemaPrimitive to DataType so that if anything changes, we won't have
  // hard-to-decipher errors in our tests. Once we get rid of Schema, we can drop this test.
  @Test
  fun testConversionFromJsonSchemaPrimitiveToDataType() {
    Assertions.assertEquals(5, DataType::class.java.getEnumConstants().size)
    Assertions.assertEquals(17, JsonSchemaPrimitive::class.java.getEnumConstants().size)

    Assertions.assertEquals(DataType.STRING, DataType.fromValue(JsonSchemaPrimitive.STRING.toString().lowercase(Locale.getDefault())))
    Assertions.assertEquals(DataType.NUMBER, DataType.fromValue(JsonSchemaPrimitive.NUMBER.toString().lowercase(Locale.getDefault())))
    Assertions.assertEquals(DataType.BOOLEAN, DataType.fromValue(JsonSchemaPrimitive.BOOLEAN.toString().lowercase(Locale.getDefault())))
    Assertions.assertEquals(DataType.ARRAY, DataType.fromValue(JsonSchemaPrimitive.ARRAY.toString().lowercase(Locale.getDefault())))
    Assertions.assertEquals(DataType.OBJECT, DataType.fromValue(JsonSchemaPrimitive.OBJECT.toString().lowercase(Locale.getDefault())))
    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) {
      DataType.fromValue(
        JsonSchemaPrimitive.NULL.toString().lowercase(
          Locale.getDefault(),
        ),
      )
    }
  }
}
