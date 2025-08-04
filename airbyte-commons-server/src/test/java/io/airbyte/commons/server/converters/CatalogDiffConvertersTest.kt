/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.protocol.transformmodels.FieldTransformType
import io.airbyte.commons.protocol.transformmodels.StreamTransformType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class CatalogDiffConvertersTest {
  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(
      isCompatible<io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum, StreamTransformType>(),
    )
    Assertions.assertTrue(
      isCompatible<io.airbyte.api.model.generated.FieldTransform.TransformTypeEnum, FieldTransformType>(),
    )
  }
}
