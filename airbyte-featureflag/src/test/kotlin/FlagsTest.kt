/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

internal class FlagsTest {
  @Test
  fun `SCIM provisioning pilot uses the approved key and defaults off`() {
    assertEquals("platform.scim-provisioning-pilot", ScimProvisioningPilot.key)
    assertFalse(ScimProvisioningPilot.default)
  }
}
