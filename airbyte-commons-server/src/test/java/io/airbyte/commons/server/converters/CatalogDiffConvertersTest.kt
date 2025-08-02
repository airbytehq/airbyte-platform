/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.protocol.transformmodels.FieldTransformType
import io.airbyte.commons.protocol.transformmodels.StreamTransformType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class CatalogDiffConvertersTest {
  @Test
  fun testEnumConversion() {
    Assertions.assertTrue(
      Enums.isCompatible<StreamTransform.TransformTypeEnum?, StreamTransformType?>(
        StreamTransform.TransformTypeEnum::class.java,
        StreamTransformType::class.java,
      ),
    )
    Assertions.assertTrue(
      Enums.isCompatible<FieldTransform.TransformTypeEnum?, FieldTransformType?>(
        FieldTransform.TransformTypeEnum::class.java,
        FieldTransformType::class.java,
      ),
    )
  }
}
