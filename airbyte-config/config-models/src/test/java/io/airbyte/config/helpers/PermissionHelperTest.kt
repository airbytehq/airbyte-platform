/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.Permission
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Set

internal class PermissionHelperTest {
  @Test
  fun allPermissionTypesHaveDefinedRules() {
    // If this assertion fails, it means a new PermissionType was added without defining the rules for
    // which permissions it grants access to. Add the new permission type to the map in
    // PermissionHelper (with appropriate values of course) in order to make this test pass again.
    Assertions.assertEquals(
      PermissionHelper.GRANTED_PERMISSION_TYPES_BY_DEFINED_PERMISSION_TYPE.keys,
      Set.of<Permission.PermissionType?>(*Permission.PermissionType.entries.toTypedArray()),
    )
  }
}
