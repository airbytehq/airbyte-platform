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

  @Test
  fun `resolves the maximum workspaces entitlement by feature ID`() {
    assertSame(MaximumWorkspacesEntitlement, Entitlements.fromId("feature-maximum-workspaces"))
  }

  @Test
  fun `resolves the plan name entitlement by feature ID`() {
    assertSame(PlanNameEntitlement, Entitlements.fromId("feature-plan-name"))
  }
}
