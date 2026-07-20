/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class EntitlementsTest {
  @Test
  fun `resolves the SCIM entitlement by feature ID`() {
    assertSame(ScimEntitlement, Entitlements.fromId("feature-scim"))
  }
}
