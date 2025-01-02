/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class AirbyteLicenseTest {
  @Test
  fun testDeserializeTypeOnlyLicense() {
    val result =
      Jsons.deserialize(
        """{ "type": "pro" } """,
        AirbyteLicense::class.java,
      )
    Assertions.assertEquals(result.type, AirbyteLicense.LicenseType.PRO)
    Assertions.assertNull(result.expirationDate)
    Assertions.assertNull(result.maxEditors)
    Assertions.assertNull(result.maxNodes)
  }
}
